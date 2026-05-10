// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package iceberg

import (
	"context"
	stderrors "errors"
	"io/fs"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	iceberggo "github.com/apache/iceberg-go"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type appendCall struct {
	identifier []string
	rows       []map[string]any
	props      iceberggo.Properties
}

type recordingAppendWriter struct {
	mu        sync.Mutex
	calls     []appendCall
	err       error
	committed map[string]map[string]struct{}
	lookups   map[string]int
}

func (r *recordingAppendWriter) AppendRows(
	_ context.Context,
	identifier []string,
	rows []map[string]any,
	props iceberggo.Properties,
) error {
	r.mu.Lock()
	err := r.err
	r.mu.Unlock()
	if err != nil {
		return err
	}
	call := appendCall{
		identifier: append([]string(nil), identifier...),
		rows:       append([]map[string]any(nil), rows...),
		props:      props,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
	if r.committed == nil {
		r.committed = make(map[string]map[string]struct{})
	}
	key := identifierKey(identifier)
	if r.committed[key] == nil {
		r.committed[key] = make(map[string]struct{})
	}
	for _, batchID := range batchIDsFromProps(props) {
		r.committed[key][batchID] = struct{}{}
	}
	return nil
}

func (r *recordingAppendWriter) CommittedBatches(
	_ context.Context,
	identifier []string,
) (map[string]struct{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lookups == nil {
		r.lookups = make(map[string]int)
	}
	r.lookups[identifierKey(identifier)]++
	if r.err != nil {
		return nil, r.err
	}
	if r.committed == nil {
		return nil, nil
	}
	committedBatches := make(map[string]struct{}, len(r.committed[identifierKey(identifier)]))
	for batchID := range r.committed[identifierKey(identifier)] {
		committedBatches[batchID] = struct{}{}
	}
	return committedBatches, nil
}

func (r *recordingAppendWriter) committedLookupCount(identifier []string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lookups[identifierKey(identifier)]
}

func (r *recordingAppendWriter) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *recordingAppendWriter) markCommitted(identifier []string, batchID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.committed == nil {
		r.committed = make(map[string]map[string]struct{})
	}
	key := identifierKey(identifier)
	if r.committed[key] == nil {
		r.committed[key] = make(map[string]struct{})
	}
	r.committed[key][batchID] = struct{}{}
}

func (r *recordingAppendWriter) getCalls() []appendCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]appendCall(nil), r.calls...)
}

func TestSinkFlushesRowsOnCheckpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-test"), cfg, writer)
	s.SetTableSchemaStore(nil)

	tableInfo := newPayloadTestTableInfo()
	tableInfo.TableName = common.TableName{Schema: "test", Table: "orders", TableID: 101}
	event := newSinkTestInsertEvent(tableInfo, 1, int64(1), "first")

	flushed := make(chan struct{})
	event.AddPostFlushFunc(func() { close(flushed) })

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddDMLEvent(event)
	s.AddCheckpointTs(42)

	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 1
	}, 3*time.Second, 10*time.Millisecond)

	select {
	case <-flushed:
	case <-time.After(3 * time.Second):
		t.Fatal("event was not post-flushed after append")
	}

	calls := writer.getCalls()
	require.Equal(t, []string{"test", "orders_cdc"}, calls[0].identifier)
	require.Len(t, calls[0].rows, 1)
	require.Equal(t, "42", calls[0].props["ticdc.checkpoint-ts"])
	require.Equal(t, "1", calls[0].props["ticdc.commit-ts"])
	require.Zero(t, stageFileCount(t, cfg.StagingDir))

	cancel()
	require.NoError(t, <-errCh)
}

func TestSinkSnapshotPropertiesCarryOwnerIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)
	cfg.TiCDCClusterID = "cdc-a"
	cfg.UpstreamID = 1001
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	tableInfo := newPayloadTestTableInfo()
	tableInfo.TableName = common.TableName{Schema: "test", Table: "orders", TableID: 101}
	event := newSinkTestInsertEvent(tableInfo, 1, int64(1), "first")

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddDMLEvent(event)
	s.AddCheckpointTs(42)

	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 1
	}, 3*time.Second, 10*time.Millisecond)

	props := writer.getCalls()[0].props
	require.Equal(t, changefeedID.String(), props[snapshotChangefeedKey])
	require.Equal(t, "cdc-a", props[snapshotCDCClusterIDKey])
	require.Equal(t, "1001", props[snapshotUpstreamIDKey])
	require.Equal(t,
		icebergcfg.TargetOwnerID("cdc-a", 1001, changefeedID.String()),
		props[snapshotOwnerIDKey])

	cancel()
	require.NoError(t, <-errCh)
}

func TestNonCommitterStagesRowsWhenBatchRowsReached(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1)
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-test"), cfg, writer)

	tableInfo := newPayloadTestTableInfo()
	tableInfo.TableName = common.TableName{Schema: "test", Table: "orders", TableID: 101}
	event := newSinkTestInsertEvent(tableInfo, 9, int64(2), "second")
	flushed := make(chan struct{})
	event.AddPostFlushFunc(func() { close(flushed) })

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddDMLEvent(event)

	require.Eventually(t, func() bool {
		return stageFileCount(t, cfg.StagingDir) == 1
	}, 3*time.Second, 10*time.Millisecond)
	require.Empty(t, writer.getCalls())
	select {
	case <-flushed:
	case <-time.After(3 * time.Second):
		t.Fatal("event was not post-flushed after durable staging")
	}

	cancel()
	require.NoError(t, <-errCh)
}

func TestStageFailureAfterWriteKeepsEventUnflushed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reset := setIcebergFaultHookForTest(t, icebergFaultAfterStageBeforePostFlush, func() error {
		return stderrors.New("injected iceberg error after stage before postflush")
	})
	defer reset()

	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1)
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-test"), cfg, writer)

	tableInfo := newPayloadTestTableInfo()
	tableInfo.TableName = common.TableName{Schema: "test", Table: "orders", TableID: 101}
	event := newSinkTestInsertEvent(tableInfo, 9, int64(2), "second")
	flushed := make(chan struct{})
	event.AddPostFlushFunc(func() { close(flushed) })

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddDMLEvent(event)

	var err error
	select {
	case err = <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("sink did not stop after injected stage failure")
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected iceberg error after stage before postflush")
	require.Equal(t, 1, stageFileCount(t, cfg.StagingDir))
	require.Empty(t, writer.getCalls())
	select {
	case <-flushed:
		t.Fatal("event should not be post-flushed after injected pre-postflush failure")
	default:
	}
}

func TestCommitterDrainsStagedBatchesFromMultipleWriters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	require.NoError(t, stage.Write(ctx, changefeedID.String(), []string{"test", "orders_cdc"}, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "data": map[string]any{"id": int64(1), "name": "first"}},
	}, 10))
	require.NoError(t, stage.Write(ctx, changefeedID.String(), []string{"test", "orders_cdc"}, []map[string]any{
		{"_op": "I", "_commit_ts": int64(11), "data": map[string]any{"id": int64(2), "name": "second"}},
	}, 11))

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(11)
	require.Eventually(t, func() bool {
		calls := writer.getCalls()
		var rows int
		for _, call := range calls {
			rows += len(call.rows)
		}
		return rows == 2
	}, 3*time.Second, 10*time.Millisecond)
	require.Zero(t, stageFileCount(t, cfg.StagingDir))

	cancel()
	require.NoError(t, <-errCh)
}

func TestCommitterSkipsAlreadyCommittedStagedBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1), "name": "first"}},
	}, 10))
	files, err := stage.List(ctx, changefeedID.String())
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.NotEmpty(t, files[0].batch.BatchID)
	writer.markCommitted(identifier, files[0].batch.BatchID)

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(10)
	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 0 && stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-errCh)
}

func TestCommitterLoadsCommittedBatchesOncePerTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	for i := 0; i < 3; i++ {
		commitTs := uint64(i + 1)
		require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
			{"_op": "I", "_commit_ts": int64(commitTs), "_table_id": int64(101), "data": map[string]any{"id": int64(i + 1)}},
		}, commitTs))
	}

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(3)
	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 1 && stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, writer.committedLookupCount(identifier))

	cancel()
	require.NoError(t, <-errCh)
}

func TestCommitterBatchesStagedFilesForSameTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(11), "_table_id": int64(101), "data": map[string]any{"id": int64(2)}},
	}, 11))

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(11)
	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 1 && len(writer.getCalls()[0].rows) == 2
	}, 3*time.Second, 10*time.Millisecond)
	calls := writer.getCalls()
	require.Len(t, batchIDsFromProps(calls[0].props), 2)
	require.Equal(t, "11", calls[0].props["ticdc.commit-ts"])
	require.Zero(t, stageFileCount(t, cfg.StagingDir))

	cancel()
	require.NoError(t, <-errCh)
}

func TestCommitterDeduplicatesOverlappingStagedRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	row1 := map[string]any{
		"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101),
		"data": map[string]any{"id": int64(1)},
	}
	row2 := map[string]any{
		"_op": "I", "_commit_ts": int64(11), "_table_id": int64(101),
		"data": map[string]any{"id": int64(2)},
	}
	row3 := map[string]any{
		"_op": "I", "_commit_ts": int64(12), "_table_id": int64(101),
		"data": map[string]any{"id": int64(3)},
	}
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{row1, row2}, 11))
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{row2, row3}, 12))

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(12)
	require.Eventually(t, func() bool {
		calls := writer.getCalls()
		return len(calls) == 1 && len(calls[0].rows) == 3
	}, 3*time.Second, 10*time.Millisecond)
	calls := writer.getCalls()
	require.Len(t, batchIDsFromProps(calls[0].props), 2)
	require.Zero(t, stageFileCount(t, cfg.StagingDir))

	cancel()
	require.NoError(t, <-errCh)
}

func TestCommitterPreservesDistinctRowsWithSamePayload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	rowA := map[string]any{
		"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101),
		"data":            map[string]any{"name": "same"},
		stagingRowIDField: "event-row-a",
	}
	rowB := map[string]any{
		"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101),
		"data":            map[string]any{"name": "same"},
		stagingRowIDField: "event-row-b",
	}
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{rowA}, 10))
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{rowB}, 11))

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(11)
	require.Eventually(t, func() bool {
		calls := writer.getCalls()
		return len(calls) == 1 && len(calls[0].rows) == 2
	}, 3*time.Second, 10*time.Millisecond)
	for _, row := range writer.getCalls()[0].rows {
		require.NotContains(t, row, stagingRowIDField)
	}
	require.Zero(t, stageFileCount(t, cfg.StagingDir))

	cancel()
	require.NoError(t, <-errCh)
}

func TestCommitterSplitsLargeDrainGroups(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	for i := 0; i < maxStagedFilesPerCommit+1; i++ {
		commitTs := uint64(i + 1)
		require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
			{"_op": "I", "_commit_ts": int64(commitTs), "_table_id": int64(101), "data": map[string]any{"id": int64(i + 1)}},
		}, commitTs))
	}

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(uint64(maxStagedFilesPerCommit + 1))
	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 2 && stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)
	calls := writer.getCalls()
	require.Len(t, batchIDsFromProps(calls[0].props), maxStagedFilesPerCommit)
	require.Len(t, batchIDsFromProps(calls[1].props), 1)

	cancel()
	require.NoError(t, <-errCh)
}

func TestCommitterFailureAfterAppendKeepsStageFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reset := setIcebergFaultHookForTest(t, icebergFaultAfterAppendBeforeStageDelete, func() error {
		return stderrors.New("injected iceberg error after append before stage delete")
	})
	defer reset()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	require.NoError(t, stage.Write(ctx, changefeedID.String(), []string{"test", "orders_cdc"}, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "data": map[string]any{"id": int64(1), "name": "first"}},
	}, 10))

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(10)

	var err error
	select {
	case err = <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("sink did not stop after injected append failure")
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected iceberg error after append before stage delete")
	require.Len(t, writer.getCalls(), 1)
	require.Len(t, batchIDsFromProps(writer.getCalls()[0].props), 1)
	require.Equal(t, 1, stageFileCount(t, cfg.StagingDir))
}

func TestCommitterDoesNotAppendBeforeCheckpointCoversStageFile(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	require.NoError(t, stage.Write(ctx, changefeedID.String(), []string{"test", "orders_cdc"}, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	require.NoError(t, s.drainStaged(ctx, 0))
	require.Empty(t, writer.getCalls())
	require.Equal(t, 1, stageFileCount(t, cfg.StagingDir))

	require.NoError(t, s.drainStaged(ctx, 9))
	require.Empty(t, writer.getCalls())
	require.Equal(t, 1, stageFileCount(t, cfg.StagingDir))

	require.NoError(t, s.drainStaged(ctx, 10))
	require.Len(t, writer.getCalls(), 1)
	require.Zero(t, stageFileCount(t, cfg.StagingDir))
}

func TestCommitterRetriesAppendFailureWithoutStopping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	writer.setErr(stderrors.New("catalog unavailable"))
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	require.NoError(t, stage.Write(ctx, changefeedID.String(), []string{"test", "orders_cdc"}, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(10)
	require.Never(t, func() bool {
		select {
		case <-errCh:
			return true
		default:
			return false
		}
	}, 200*time.Millisecond, 10*time.Millisecond)
	require.Equal(t, 1, stageFileCount(t, cfg.StagingDir))

	writer.setErr(nil)
	s.AddCheckpointTs(10)
	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 1 && stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-errCh)
}

func TestCommitterLeavesFutureStagedBatchesUntilCheckpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	require.NoError(t, stage.Write(ctx, changefeedID.String(), []string{"test", "orders_cdc"}, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "data": map[string]any{"id": int64(1)}},
	}, 10))
	require.NoError(t, stage.Write(ctx, changefeedID.String(), []string{"test", "orders_cdc"}, []map[string]any{
		{"_op": "I", "_commit_ts": int64(20), "data": map[string]any{"id": int64(2)}},
	}, 20))

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(10)
	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 1 && stageFileCount(t, cfg.StagingDir) == 1
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, "10", writer.getCalls()[0].props["ticdc.checkpoint-ts"])

	s.AddCheckpointTs(20)
	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 2 && stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-errCh)
}

func TestAddCheckpointTsUpdatesLatestWhenCommandChannelFull(t *testing.T) {
	ctx := context.Background()
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-test"), cfg, writer)
	s.inputCh = make(chan sinkCommand, 1)
	s.inputCh <- sinkCommand{}

	s.AddCheckpointTs(99)

	require.Equal(t, uint64(99), s.latestCheckpointTs.Load())
}

func TestSinkRecordsIcebergStagedBacklogMetrics(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-metrics")
	s := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})
	defer s.closeMetrics()

	stagedFiles := []stagedFile{
		{batch: stagedBatch{Rows: []map[string]any{{"id": 1}, {"id": 2}}}},
		{batch: stagedBatch{Rows: []map[string]any{{"id": 3}}}},
	}
	eligible := stagedFiles[:1]

	s.recordStageMetrics(stagedFiles, eligible)

	require.Equal(t, float64(2), testutil.ToFloat64(
		metrics.IcebergStagedFilesGauge.WithLabelValues("default", "iceberg-metrics", "pending")))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.IcebergStagedFilesGauge.WithLabelValues("default", "iceberg-metrics", "eligible")))
	require.Equal(t, float64(3), testutil.ToFloat64(
		metrics.IcebergStagedRowsGauge.WithLabelValues("default", "iceberg-metrics", "pending")))
	require.Equal(t, float64(2), testutil.ToFloat64(
		metrics.IcebergStagedRowsGauge.WithLabelValues("default", "iceberg-metrics", "eligible")))
}

func TestClosePreservesOwnerConflictMetricUntilRemove(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-conflict-metric")
	s := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})
	defer s.closeMetrics()

	s.recordTargetOwnerConflict(errIcebergTargetOwnerConflict)
	s.Close(false)

	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.IcebergTargetOwnerConflictCounter.WithLabelValues("default", "iceberg-conflict-metric")))

	s.Close(true)
	require.Equal(t, float64(0), testutil.ToFloat64(
		metrics.IcebergTargetOwnerConflictCounter.WithLabelValues("default", "iceberg-conflict-metric")))
}

func TestStageTargetOwnerClaimRejectsDifferentChangefeed(t *testing.T) {
	ctx := context.Background()
	stage := newStageStore(t.TempDir())
	identifier := []string{"test", "orders_cdc"}

	require.NoError(t, stage.ClaimTargetOwner(ctx, "default/left", identifier))
	require.NoError(t, stage.ClaimTargetOwner(ctx, "default/left", identifier))

	err := stage.ClaimTargetOwner(ctx, "default/right", identifier)
	require.Error(t, err)
	require.Equal(t, errIcebergTargetOwnerConflict, cerror.Cause(err))
}

func TestCommitterRejectsTargetOwnerClaimBeforeWriterInit(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	left := common.NewChangefeedID4Test("default", "iceberg-left")
	right := common.NewChangefeedID4Test("default", "iceberg-right")

	require.NoError(t, stage.ClaimTargetOwner(ctx, left.String(), identifier))
	require.NoError(t, stage.Write(ctx, right.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))

	writer := &recordingAppendWriter{}
	s := newSink(ctx, right, cfg, writer)
	s.SetTableSchemaStore(nil)
	defer s.closeMetrics()

	err := s.drainStaged(ctx, 10)
	require.Error(t, err)
	require.Equal(t, errIcebergTargetOwnerConflict, cerror.Cause(err))
	require.Empty(t, writer.getCalls())
}

func TestCommitterRejectsWarehouseTargetOwnerClaimAcrossClusters(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	cfg.TiCDCClusterID = "cdc-b"
	cfg.UpstreamID = 2002
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	left := common.NewChangefeedID4Test("default", "iceberg-left")
	right := common.NewChangefeedID4Test("default", "iceberg-right-warehouse")

	require.NoError(t, icebergcfg.ClaimTargetOwner(ctx, cfg.Warehouse,
		icebergcfg.NewTargetOwnerClaim("cdc-a", 1001, left.String(), identifier)))
	require.NoError(t, stage.Write(ctx, right.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))

	writer := &recordingAppendWriter{}
	s := newSink(ctx, right, cfg, writer)
	s.SetTableSchemaStore(nil)
	defer s.closeMetrics()

	err := s.drainStaged(ctx, 10)
	require.Error(t, err)
	require.True(t, cerror.Is(err, errIcebergTargetOwnerConflict))
	require.Empty(t, writer.getCalls())
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.IcebergTargetOwnerConflictCounter.WithLabelValues("default", "iceberg-right-warehouse")))
}

func TestCloseRemoveChangefeedDeletesOnlyOwnStaging(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	otherIdentifier := []string{"test", "customers_cdc"}
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	otherChangefeed := common.NewChangefeedID4Test("default", "iceberg-other").String()

	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))
	require.NoError(t, stage.Write(ctx, otherChangefeed, identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(11), "_table_id": int64(101), "data": map[string]any{"id": int64(2)}},
	}, 11))
	require.NoError(t, stage.ClaimTargetOwner(ctx, changefeedID.String(), identifier))
	require.NoError(t, stage.ClaimTargetOwner(ctx, otherChangefeed, otherIdentifier))
	require.Equal(t, 2, stageFileCount(t, cfg.StagingDir))

	s := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})
	s.Close(true)

	ownFiles, err := stage.List(ctx, changefeedID.String())
	require.NoError(t, err)
	require.Empty(t, ownFiles)

	otherFiles, err := stage.List(ctx, otherChangefeed)
	require.NoError(t, err)
	require.Len(t, otherFiles, 1)
	require.Equal(t, 1, stageFileCount(t, cfg.StagingDir))
	require.NoError(t, stage.ClaimTargetOwner(ctx, "default/new-owner", identifier))

	err = stage.ClaimTargetOwner(ctx, "default/new-owner", otherIdentifier)
	require.Error(t, err)
	require.Equal(t, errIcebergTargetOwnerConflict, cerror.Cause(err))
}

func TestCloseKeepsStagingWhenNotRemovingChangefeed(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")

	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))
	require.NoError(t, stage.ClaimTargetOwner(ctx, changefeedID.String(), identifier))

	s := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})
	s.Close(false)

	files, err := stage.List(ctx, changefeedID.String())
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, 1, stageFileCount(t, cfg.StagingDir))
	err = stage.ClaimTargetOwner(ctx, "default/new-owner", identifier)
	require.Error(t, err)
	require.Equal(t, errIcebergTargetOwnerConflict, cerror.Cause(err))
}

func TestSinkPrefixesDatabase(t *testing.T) {
	ctx := context.Background()
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1)
	cfg.DatabasePrefix = "ticdc_"
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-test"), cfg, writer)
	s.SetTableSchemaStore(nil)

	tableInfo := newPayloadTestTableInfo()
	tableInfo.TableName = common.TableName{Schema: "shop", Table: "customers", TableID: 101}
	event := newSinkTestInsertEvent(tableInfo, 11, int64(3), "third")

	errCh := make(chan error, 1)
	runCtx, cancel := context.WithCancel(ctx)
	go func() {
		errCh <- s.Run(runCtx)
	}()

	s.AddDMLEvent(event)
	s.AddCheckpointTs(11)
	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 1
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"ticdc_shop", "customers_cdc"}, writer.getCalls()[0].identifier)

	cancel()
	require.NoError(t, <-errCh)
}

func TestStageWriteIsDeterministicForSameLogicalBatch(t *testing.T) {
	ctx := context.Background()
	stage := newStageStore(t.TempDir())
	changefeed := "default/iceberg-test"
	identifier := []string{"test", "orders_cdc"}
	rows := []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1), "name": "first"}},
	}

	require.NoError(t, stage.Write(ctx, changefeed, identifier, rows, 10))
	require.NoError(t, stage.Write(ctx, changefeed, identifier, rows, 10))

	files, err := stage.List(ctx, changefeed)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.NotEmpty(t, files[0].batch.BatchID)
}

func TestStageWriteIsDeterministicAcrossReplaySeq(t *testing.T) {
	ctx := context.Background()
	stage := newStageStore(t.TempDir())
	changefeed := "default/iceberg-test"
	identifier := []string{"test", "orders_cdc"}
	firstRows := []map[string]any{
		{
			"_op": "I", "_commit_ts": int64(10), "_seq": int64(1), "_table_id": int64(101),
			"data":            map[string]any{"id": int64(1), "name": "first"},
			stagingRowIDField: "stable-row-id",
		},
	}
	replayedRows := []map[string]any{
		{
			"_op": "I", "_commit_ts": int64(10), "_seq": int64(9), "_table_id": int64(101),
			"data":            map[string]any{"id": int64(1), "name": "first"},
			stagingRowIDField: "stable-row-id",
		},
	}

	require.NoError(t, stage.Write(ctx, changefeed, identifier, firstRows, 10))
	require.NoError(t, stage.Write(ctx, changefeed, identifier, replayedRows, 10))

	files, err := stage.List(ctx, changefeed)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, []string{"stable-row-id"}, files[0].batch.RowIDs)
}

func TestStagingRowIDIgnoresReplaySplitIndexWhenRowKeyIsPresent(t *testing.T) {
	row := map[string]any{
		"_op":        "I",
		"_commit_ts": int64(10),
		"_start_ts":  int64(9),
		"_table_id":  int64(101),
		"data":       map[string]any{"id": int64(1), "name": "first"},
	}

	first, err := stagingRowIDForEvent(101, 9, 10, 0, []byte("pk:1"), row)
	require.NoError(t, err)
	replayed, err := stagingRowIDForEvent(101, 9, 10, 7, []byte("pk:1"), row)
	require.NoError(t, err)

	require.Equal(t, first, replayed)
}

func TestStagingRowIDKeepsSplitIndexWhenRowKeyIsAbsent(t *testing.T) {
	row := map[string]any{
		"_op":        "I",
		"_commit_ts": int64(10),
		"_start_ts":  int64(9),
		"_table_id":  int64(101),
		"data":       map[string]any{"name": "same"},
	}

	first, err := stagingRowIDForEvent(101, 9, 10, 0, nil, row)
	require.NoError(t, err)
	replayed, err := stagingRowIDForEvent(101, 9, 10, 7, nil, row)
	require.NoError(t, err)

	require.NotEqual(t, first, replayed)
}

func TestWriteBlockEventRejectsDDL(t *testing.T) {
	ctx := context.Background()
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1)
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-test"), cfg, writer)

	ddl := &commonEvent.DDLEvent{Query: "truncate table orders", FinishedTs: 42}
	flushed := false
	ddl.AddPostFlushFunc(func() { flushed = true })

	err := s.WriteBlockEvent(ddl)
	require.Error(t, err)
	require.Contains(t, err.Error(), "iceberg sink does not support block event")
	require.False(t, flushed)
}

func TestWriteBlockEventAllowsCreateDDL(t *testing.T) {
	ctx := context.Background()
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1)
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-test"), cfg, writer)

	ddl := &commonEvent.DDLEvent{
		Type:       byte(model.ActionCreateTable),
		Query:      "create table orders(id bigint primary key)",
		FinishedTs: 42,
	}
	flushed := false
	ddl.AddPostFlushFunc(func() { flushed = true })

	require.NoError(t, s.WriteBlockEvent(ddl))
	require.True(t, flushed)
}

func newSinkTestConfig(t *testing.T, batchRows int) *icebergcfg.Config {
	t.Helper()
	return &icebergcfg.Config{
		CatalogURI:     "http://localhost:8181/",
		Warehouse:      (&url.URL{Scheme: "file", Path: t.TempDir()}).String(),
		StagingDir:     t.TempDir(),
		CommitInterval: time.Hour,
		BatchRows:      batchRows,
		TableSuffix:    "_cdc",
	}
}

func setIcebergFaultHookForTest(t *testing.T, name icebergFaultHookName, hook func() error) func() {
	t.Helper()
	icebergFaultHooks.Lock()
	var previous func() error
	switch name {
	case icebergFaultAfterStageBeforePostFlush:
		previous = icebergFaultHooks.afterStageBeforePostFlush
		icebergFaultHooks.afterStageBeforePostFlush = hook
	case icebergFaultAfterAppendBeforeStageDelete:
		previous = icebergFaultHooks.afterAppendBeforeStageDelete
		icebergFaultHooks.afterAppendBeforeStageDelete = hook
	default:
		t.Fatalf("unknown iceberg fault hook: %d", name)
	}
	icebergFaultHooks.Unlock()
	return func() {
		icebergFaultHooks.Lock()
		defer icebergFaultHooks.Unlock()
		switch name {
		case icebergFaultAfterStageBeforePostFlush:
			icebergFaultHooks.afterStageBeforePostFlush = previous
		case icebergFaultAfterAppendBeforeStageDelete:
			icebergFaultHooks.afterAppendBeforeStageDelete = previous
		}
	}
}

func stageFileCount(t *testing.T, root string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		if d == nil || d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		count++
		return nil
	})
	require.NoError(t, err)
	return count
}

func newSinkTestInsertEvent(tableInfo *common.TableInfo, commitTs uint64, id int64, name string) *commonEvent.DMLEvent {
	event := newPayloadTestEvent(tableInfo, commitTs, commitTs)
	event.RowTypes = []common.RowType{common.RowTypeInsert}
	event.Length = 1
	event.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	appendPayloadTestRow(event.Rows, id, name)
	return event
}
