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
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	iceberggo "github.com/apache/iceberg-go"
	icebergrest "github.com/apache/iceberg-go/catalog/rest"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type appendCall struct {
	identifier  []string
	rows        []map[string]any
	tableSchema *stagedTableSchema
	props       iceberggo.Properties
}

type recordingAppendWriter struct {
	mu             sync.Mutex
	calls          []appendCall
	err            error
	committed      map[string]map[string]struct{}
	lookups        map[string]int
	lookupBatchIDs map[string][][]string
}

func (r *recordingAppendWriter) AppendRows(
	_ context.Context,
	identifier []string,
	rows []map[string]any,
	tableSchema *stagedTableSchema,
	props iceberggo.Properties,
) error {
	r.mu.Lock()
	err := r.err
	r.mu.Unlock()
	if err != nil {
		return err
	}
	call := appendCall{
		identifier:  append([]string(nil), identifier...),
		rows:        append([]map[string]any(nil), rows...),
		tableSchema: tableSchema,
		props:       props,
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
	batchIDs []string,
) (map[string]struct{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lookups == nil {
		r.lookups = make(map[string]int)
	}
	key := identifierKey(identifier)
	r.lookups[key]++
	if r.lookupBatchIDs == nil {
		r.lookupBatchIDs = make(map[string][][]string)
	}
	r.lookupBatchIDs[key] = append(r.lookupBatchIDs[key], append([]string(nil), batchIDs...))
	if r.err != nil {
		return nil, r.err
	}
	if r.committed == nil {
		return nil, nil
	}
	source := r.committed[key]
	committedBatches := make(map[string]struct{}, len(batchIDs))
	for _, batchID := range batchIDs {
		if _, ok := source[batchID]; ok {
			committedBatches[batchID] = struct{}{}
		}
	}
	return committedBatches, nil
}

func (r *recordingAppendWriter) committedLookupCount(identifier []string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lookups[identifierKey(identifier)]
}

func (r *recordingAppendWriter) committedLookupBatchIDs(identifier []string) [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	lookups := r.lookupBatchIDs[identifierKey(identifier)]
	copied := make([][]string, 0, len(lookups))
	for _, lookup := range lookups {
		copied = append(copied, append([]string(nil), lookup...))
	}
	return copied
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

type recordingReconcileWriter struct {
	recordingAppendWriter
	reconcileCalls [][]string
}

func (r *recordingReconcileWriter) ReconcileTargetOnTakeover(
	_ context.Context,
	identifier []string,
	_ string,
	_ string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reconcileCalls = append(r.reconcileCalls, append([]string(nil), identifier...))
	return nil
}

func (r *recordingReconcileWriter) getReconcileCalls() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]string(nil), r.reconcileCalls...)
}

type flakyCommitWriter struct {
	recordingAppendWriter
	failures int
}

type schemaUpdateCall struct {
	identifier   []string
	tableInfo    *common.TableInfo
	ddlType      model.ActionType
	ownerID      string
	changefeed   string
	cdcClusterID string
	upstreamID   string
}

type recordingSchemaAppendWriter struct {
	recordingAppendWriter
	schemaMu    sync.Mutex
	schemaCalls []schemaUpdateCall
	schemaErr   error
}

func (w *recordingSchemaAppendWriter) ApplyTableSchema(
	_ context.Context,
	identifier []string,
	tableInfo *common.TableInfo,
	ddlType model.ActionType,
	ownerID string,
	changefeed string,
	cdcClusterID string,
	upstreamID string,
) error {
	w.schemaMu.Lock()
	defer w.schemaMu.Unlock()
	if w.schemaErr != nil {
		return w.schemaErr
	}
	w.schemaCalls = append(w.schemaCalls, schemaUpdateCall{
		identifier:   append([]string(nil), identifier...),
		tableInfo:    tableInfo,
		ddlType:      ddlType,
		ownerID:      ownerID,
		changefeed:   changefeed,
		cdcClusterID: cdcClusterID,
		upstreamID:   upstreamID,
	})
	return nil
}

func (w *recordingSchemaAppendWriter) getSchemaCalls() []schemaUpdateCall {
	w.schemaMu.Lock()
	defer w.schemaMu.Unlock()
	return append([]schemaUpdateCall(nil), w.schemaCalls...)
}

func (w *flakyCommitWriter) AppendRows(
	ctx context.Context,
	identifier []string,
	rows []map[string]any,
	tableSchema *stagedTableSchema,
	props iceberggo.Properties,
) error {
	w.mu.Lock()
	if w.failures > 0 {
		w.failures--
		w.mu.Unlock()
		return icebergrest.ErrCommitFailed
	}
	w.mu.Unlock()
	return w.recordingAppendWriter.AppendRows(ctx, identifier, rows, tableSchema, props)
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
		return len(writer.getCalls()) == 1 && stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

	select {
	case <-flushed:
	case <-time.After(3 * time.Second):
		t.Fatal("event was not post-flushed after append")
	}

	calls := writer.getCalls()
	require.Equal(t, []string{"test", "orders_cdc"}, calls[0].identifier)
	require.Len(t, calls[0].rows, 1)
	require.NotContains(t, calls[0].props, "ticdc.checkpoint-ts")
	require.Equal(t, "42", calls[0].props[snapshotCommitBarrierTsKey])
	require.Equal(t, "1", calls[0].props["ticdc.commit-ts"])
	require.Eventually(t, func() bool {
		return stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

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

func TestStageFileCarriesTableInfoSchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1)
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-test"), cfg, writer)

	tableInfo := newPayloadTestTableInfo()
	tableInfo.TableName = common.TableName{Schema: "test", Table: "orders", TableID: 101}
	event := newSinkTestInsertEvent(tableInfo, 9, int64(2), "")

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddDMLEvent(event)
	require.Eventually(t, func() bool {
		return stageFileCount(t, cfg.StagingDir) == 1
	}, 3*time.Second, 10*time.Millisecond)

	stage := newStageStore(cfg.StagingDir)
	files, err := stage.List(ctx, s.changefeedID.String())
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.NotNil(t, files[0].batch.TableSchema)
	require.Equal(t, []stagedColumnSchema{
		{Name: "id", Type: stagedColumnTypeInt64},
		{Name: "name", Type: stagedColumnTypeString},
	}, files[0].batch.TableSchema.Columns)

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
	require.Eventually(t, func() bool {
		return stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

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

func TestCommitterSkipsLedgerCommittedBatchAfterSnapshotHistoryExpires(t *testing.T) {
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
	_, err = stage.MarkBatchesCommitted(ctx, changefeedID.String(), identifier, []string{files[0].batch.BatchID}, 10, 1)
	require.NoError(t, err)

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
	files, err := stage.List(ctx, changefeedID.String())
	require.NoError(t, err)
	expectedBatchIDs := make([]string, 0, len(files))
	for _, file := range files {
		expectedBatchIDs = append(expectedBatchIDs, file.batch.BatchID)
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
	lookupBatchIDs := writer.committedLookupBatchIDs(identifier)
	require.Len(t, lookupBatchIDs, 1)
	require.ElementsMatch(t, expectedBatchIDs, lookupBatchIDs[0])

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
	require.Eventually(t, func() bool {
		return stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

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
	require.Eventually(t, func() bool {
		return stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-errCh)
}

func TestCommitterDeduplicatesRowsCommittedInPriorDrain(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{
			"_op":             "I",
			"_commit_ts":      int64(10),
			"_table_id":       int64(101),
			stagingRowIDField: "stable-replay-row",
			"data":            map[string]any{"id": int64(1)},
		},
	}, 10))
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{
			"_op":             "I",
			"_commit_ts":      int64(10),
			"_seq":            int64(7),
			"_table_id":       int64(101),
			stagingRowIDField: "stable-replay-row",
			"data":            map[string]any{"id": int64(1)},
		},
		{
			"_op":             "I",
			"_commit_ts":      int64(20),
			"_table_id":       int64(101),
			stagingRowIDField: "new-row",
			"data":            map[string]any{"id": int64(2)},
		},
	}, 20))

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	require.NoError(t, s.drainStaged(ctx, 10))
	require.Len(t, writer.getCalls(), 1)
	require.Equal(t, 2, stageFileCount(t, cfg.StagingDir))

	restarted := newSink(ctx, changefeedID, cfg, writer)
	restarted.SetTableSchemaStore(nil)

	require.NoError(t, restarted.drainStaged(ctx, 20))
	calls := writer.getCalls()
	require.Len(t, calls, 2)
	require.Len(t, calls[1].rows, 1)
	data := calls[1].rows[0]["data"].(map[string]any)
	require.Equal(t, "2", data["id"].(json.Number).String())
	require.Zero(t, stageFileCount(t, cfg.StagingDir))
}

func TestCommitterDeduplicatesDelayedReplayAfterCleanup(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{
			"_op":             "I",
			"_commit_ts":      int64(10),
			"_table_id":       int64(101),
			stagingRowIDField: "stable-replay-row",
			"data":            map[string]any{"id": int64(1)},
		},
	}, 10))

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)
	require.NoError(t, s.drainStaged(ctx, 10))
	require.Len(t, writer.getCalls(), 1)
	require.Zero(t, stageFileCount(t, cfg.StagingDir))

	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{
			"_op":             "I",
			"_commit_ts":      int64(20),
			"_table_id":       int64(101),
			stagingRowIDField: "stable-replay-row",
			"data":            map[string]any{"id": int64(1)},
		},
		{
			"_op":             "I",
			"_commit_ts":      int64(20),
			"_table_id":       int64(101),
			stagingRowIDField: "new-row",
			"data":            map[string]any{"id": int64(2)},
		},
	}, 20))

	restarted := newSink(ctx, changefeedID, cfg, writer)
	restarted.SetTableSchemaStore(nil)
	require.NoError(t, restarted.drainStaged(ctx, 20))
	calls := writer.getCalls()
	require.Len(t, calls, 2)
	require.Len(t, calls[1].rows, 1)
	data := calls[1].rows[0]["data"].(map[string]any)
	require.Equal(t, "2", data["id"].(json.Number).String())
	require.Zero(t, stageFileCount(t, cfg.StagingDir))
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
	require.Eventually(t, func() bool {
		return stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

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

func TestCommitterWritesDurableLedgerBeforeDeletingStageFile(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))
	files, err := stage.List(ctx, changefeedID.String())
	require.NoError(t, err)
	require.Len(t, files, 1)
	batchID := files[0].batch.BatchID

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	require.NoError(t, s.drainStaged(ctx, 10))
	require.Len(t, writer.getCalls(), 1)
	require.Zero(t, stageFileCount(t, cfg.StagingDir))

	ledger, err := stage.CommittedBatchesForCandidates(ctx, changefeedID.String(), identifier, []string{batchID})
	require.NoError(t, err)
	require.Contains(t, ledger, batchID)
}

func TestCommitterMarksDuplicateOnlyReplayBatchCommitted(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)

	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{
			"_op":             "I",
			"_commit_ts":      int64(10),
			"_table_id":       int64(101),
			stagingRowIDField: "stable-replay-row",
			"data":            map[string]any{"id": int64(1)},
		},
	}, 10))
	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{
			"_op":             "I",
			"_commit_ts":      int64(20),
			"_table_id":       int64(101),
			stagingRowIDField: "stable-replay-row",
			"data":            map[string]any{"id": int64(1)},
		},
	}, 20))
	files, err := stage.List(ctx, changefeedID.String())
	require.NoError(t, err)
	require.Len(t, files, 2)
	var duplicateOnlyBatchID string
	for _, file := range files {
		if file.batch.MaxCommitTs == 20 {
			duplicateOnlyBatchID = file.batch.BatchID
		}
	}
	require.NotEmpty(t, duplicateOnlyBatchID)

	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)
	require.NoError(t, s.drainStaged(ctx, 10))
	require.Len(t, writer.getCalls(), 1)
	require.Equal(t, 2, stageFileCount(t, cfg.StagingDir))

	restarted := newSink(ctx, changefeedID, cfg, writer)
	restarted.SetTableSchemaStore(nil)
	require.NoError(t, restarted.drainStaged(ctx, 20))
	require.Len(t, writer.getCalls(), 1)
	require.Zero(t, stageFileCount(t, cfg.StagingDir))

	ledger, err := stage.CommittedBatchesForCandidates(ctx, changefeedID.String(), identifier, []string{duplicateOnlyBatchID})
	require.NoError(t, err)
	require.Contains(t, ledger, duplicateOnlyBatchID)
}

func TestStageListSkipsCommittedLedgerSubtree(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}

	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))
	created, err := stage.MarkBatchesCommitted(ctx, changefeedID.String(), identifier, []string{"already-committed"}, 10, 1)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	require.NoError(t, os.WriteFile(
		filepath.Join(committedBatchLedgerDir(cfg.StagingDir, changefeedID.String(), identifier), "not-a-stage-file.json"),
		[]byte("{"),
		0o644))
	rowLedgerDir := committedRowSegmentLedgerDir(cfg.StagingDir, changefeedID.String(), identifier, "0")
	require.NoError(t, os.MkdirAll(rowLedgerDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(rowLedgerDir, "not-a-stage-file.json"), []byte("{"), 0o644))

	files, err := stage.List(ctx, changefeedID.String())
	require.NoError(t, err)
	require.Len(t, files, 1)
}

func TestStageWriteSyncsFileAndDirectoryBeforeReturning(t *testing.T) {
	ctx := context.Background()

	oldSyncFile := syncFileForAtomicWrite
	oldSyncDir := syncDirForAtomicWrite
	defer func() {
		syncFileForAtomicWrite = oldSyncFile
		syncDirForAtomicWrite = oldSyncDir
	}()

	fileSynced := false
	var syncedDirs []string
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	cfg := newSinkTestConfig(t, 1024)
	identifier := []string{"test", "orders_cdc"}
	changefeedDir := filepath.Join(cfg.StagingDir, pathSegment(changefeedID.String()))
	targetDir := filepath.Join(changefeedDir, pathSegment(identifierKey(identifier)))

	syncFileForAtomicWrite = func(file *os.File) error {
		require.NotEmpty(t, file.Name())
		fileSynced = true
		return nil
	}
	syncDirForAtomicWrite = func(path string) error {
		if path == targetDir {
			require.True(t, fileSynced, "target directory sync must happen after file sync")
		}
		syncedDirs = append(syncedDirs, path)
		return nil
	}

	stage := newStageStore(cfg.StagingDir)

	require.NoError(t, stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))

	require.True(t, fileSynced)
	require.Contains(t, syncedDirs, cfg.StagingDir)
	require.Contains(t, syncedDirs, changefeedDir)
	require.Contains(t, syncedDirs, targetDir)
}

func TestStageCommittedBatchesUsesCandidateLookup(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}

	created, err := stage.MarkBatchesCommitted(ctx, changefeedID.String(), identifier, []string{"batch-a"}, 10, 1)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	created, err = stage.MarkBatchesCommitted(ctx, changefeedID.String(), identifier, []string{"batch-a"}, 10, 1)
	require.NoError(t, err)
	require.Zero(t, created)
	require.NoError(t, os.WriteFile(
		filepath.Join(committedBatchLedgerDir(cfg.StagingDir, changefeedID.String(), identifier), "corrupt-unrequested.commit"),
		[]byte("{"),
		0o644))

	ledger, err := stage.CommittedBatchesForCandidates(ctx, changefeedID.String(), identifier, []string{"batch-a", "missing"})
	require.NoError(t, err)
	require.Contains(t, ledger, "batch-a")
	require.NotContains(t, ledger, "missing")
}

func TestStageCommittedRowIDsUsesCandidateLookup(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}

	created, err := stage.MarkRowsCommitted(ctx, changefeedID.String(), identifier, []string{"row-a", "row-b", "row-a"}, []string{"batch-a"}, 10)
	require.NoError(t, err)
	require.Equal(t, 2, created)
	created, err = stage.MarkRowsCommitted(ctx, changefeedID.String(), identifier, []string{"row-a"}, []string{"batch-a"}, 10)
	require.NoError(t, err)
	require.Zero(t, created)
	corruptRowID := "row-c"
	for committedRowLedgerBucket(corruptRowID) == committedRowLedgerBucket("row-a") {
		corruptRowID += "x"
	}
	corruptDir := committedRowSegmentLedgerDir(
		cfg.StagingDir, changefeedID.String(), identifier, committedRowLedgerBucket(corruptRowID))
	require.NoError(t, os.MkdirAll(corruptDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(corruptDir, "corrupt.rows"), []byte("{"), 0o644))

	ledger, err := stage.CommittedRowIDsForCandidates(ctx, changefeedID.String(), identifier, []string{"row-a", "missing"})
	require.NoError(t, err)
	require.Contains(t, ledger, "row-a")
	require.NotContains(t, ledger, "missing")
}

func TestStageCommittedRowIDsDoesNotDecodeRetainedSegmentsForMisses(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	missing := "candidate-new-row"
	missingShard := committedRowIndexShard(missing)

	for i := 0; i < 200; i++ {
		rowID := rowIDInIndexShard(fmt.Sprintf("retained-row-%04d", i), missingShard)
		created, err := stage.MarkRowsCommitted(ctx, changefeedID.String(), identifier, []string{rowID}, []string{fmt.Sprintf("batch-%04d", i)}, uint64(i+1))
		require.NoError(t, err)
		require.Equal(t, 1, created)
	}
	corruptDir := committedRowSegmentLedgerDir(
		cfg.StagingDir, changefeedID.String(), identifier, committedRowLedgerBucket(missing))
	require.NoError(t, os.MkdirAll(corruptDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(corruptDir, "corrupt-retained.rows"), []byte("{"), 0o644))

	ledger, err := stage.CommittedRowIDsForCandidates(ctx, changefeedID.String(), identifier, []string{missing})
	require.NoError(t, err)
	require.Empty(t, ledger)
}

func TestStageCommittedRowIDsRebuildsMissingIndexFromRetainedSegments(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}

	created, err := stage.MarkRowsCommitted(ctx, changefeedID.String(), identifier, []string{"row-a"}, []string{"batch-a"}, 10)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	require.NoError(t, os.RemoveAll(committedRowIndexDir(cfg.StagingDir, changefeedID.String(), identifier)))

	ledger, err := stage.CommittedRowIDsForCandidates(ctx, changefeedID.String(), identifier, []string{"row-a", "missing"})
	require.NoError(t, err)
	require.Contains(t, ledger, "row-a")
	require.NotContains(t, ledger, "missing")

	_, err = os.Stat(committedRowIndexPath(cfg.StagingDir, changefeedID.String(), identifier))
	require.NoError(t, err)
}

func TestStageCommittedRowIDsRebuildsMissingShardIndexFromRetainedSegments(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	rowA := "row-a"
	rowB := rowIDOutsideIndexShard("row-b", committedRowIndexShard(rowA))

	created, err := stage.MarkRowsCommitted(ctx, changefeedID.String(), identifier, []string{rowA, rowB}, []string{"batch-a"}, 10)
	require.NoError(t, err)
	require.Equal(t, 2, created)
	deleteRowIndexShardForTest(t, cfg.StagingDir, changefeedID.String(), identifier, committedRowIndexShard(rowA))
	requireRowIndexShardExists(t, cfg.StagingDir, changefeedID.String(), identifier, committedRowIndexShard(rowB))

	ledger, err := stage.CommittedRowIDsForCandidates(ctx, changefeedID.String(), identifier, []string{rowA, rowB, "missing"})
	require.NoError(t, err)
	require.Contains(t, ledger, rowA)
	require.Contains(t, ledger, rowB)
	require.NotContains(t, ledger, "missing")

	requireRowIndexShardExists(t, cfg.StagingDir, changefeedID.String(), identifier, committedRowIndexShard(rowA))
}

func TestStageCommittedRowIDsRepairsDirtyShardIndexesAndClearsMarker(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	rowA := "row-a"
	rowB := rowIDOutsideIndexShard("row-b", committedRowIndexShard(rowA))

	created, err := stage.MarkRowsCommitted(ctx, changefeedID.String(), identifier, []string{rowA, rowB}, []string{"batch-a"}, 10)
	require.NoError(t, err)
	require.Equal(t, 2, created)
	deleteRowIndexShardForTest(t, cfg.StagingDir, changefeedID.String(), identifier, committedRowIndexShard(rowA))
	err = stage.markCommittedRowIndexDirty(changefeedID.String(), identifier, map[string]map[string]struct{}{
		committedRowIndexShard(rowA): {committedRowHash(rowA): {}},
		committedRowIndexShard(rowB): {committedRowHash(rowB): {}},
	}, time.Now().UTC())
	require.NoError(t, err)

	ledger, err := stage.CommittedRowIDsForCandidates(ctx, changefeedID.String(), identifier, []string{rowA, rowB})
	require.NoError(t, err)
	require.Contains(t, ledger, rowA)
	require.Contains(t, ledger, rowB)
	requireRowIndexShardExists(t, cfg.StagingDir, changefeedID.String(), identifier, committedRowIndexShard(rowA))
	requireRowIndexShardExists(t, cfg.StagingDir, changefeedID.String(), identifier, committedRowIndexShard(rowB))
	_, err = os.Stat(committedRowIndexDirtyPath(cfg.StagingDir, changefeedID.String(), identifier))
	require.True(t, os.IsNotExist(err))
}

func TestStageCommittedRowIDsBatchesFiveThousandRowsIntoSegments(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	cfg := newSinkTestConfig(t, maxRowsPerCommit)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	rowIDs := make([]string, 0, maxRowsPerCommit)
	for i := 0; i < maxRowsPerCommit; i++ {
		rowIDs = append(rowIDs, fmt.Sprintf("row-%04d", i))
	}

	created, err := stage.MarkRowsCommitted(ctx, changefeedID.String(), identifier, rowIDs, []string{"batch-a"}, 10)
	require.NoError(t, err)
	require.Equal(t, maxRowsPerCommit, created)
	require.LessOrEqual(t, rowLedgerFileCount(t, cfg.StagingDir), committedRowLedgerBucketCount)
	require.LessOrEqual(t, rowIndexFileCount(t, cfg.StagingDir), committedRowIndexShardCount)

	ledger, err := stage.CommittedRowIDsForCandidates(ctx, changefeedID.String(), identifier, []string{"row-0000", "row-4999", "missing"})
	require.NoError(t, err)
	require.Contains(t, ledger, "row-0000")
	require.Contains(t, ledger, "row-4999")
	require.NotContains(t, ledger, "missing")
}

func TestStageCommittedRowIDsDoesNotRewriteUntouchedRetainedShardIndex(t *testing.T) {
	ctx := context.Background()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-test")
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	retainedShard := committedRowIndexShard("retained-row")
	retainedRows := make([]string, 0, 200)
	for i := 0; i < cap(retainedRows); i++ {
		retainedRows = append(retainedRows, rowIDInIndexShard(fmt.Sprintf("retained-row-%04d", i), retainedShard))
	}

	created, err := stage.MarkRowsCommitted(ctx, changefeedID.String(), identifier, retainedRows, []string{"batch-retained"}, 10)
	require.NoError(t, err)
	require.Equal(t, len(retainedRows), created)
	before := rowIndexShardHashes(t, cfg.StagingDir, changefeedID.String(), identifier, retainedShard)

	newRow := rowIDOutsideIndexShard("all-new-row", retainedShard)
	created, err = stage.MarkRowsCommitted(ctx, changefeedID.String(), identifier, []string{newRow}, []string{"batch-new"}, 20)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	after := rowIndexShardHashes(t, cfg.StagingDir, changefeedID.String(), identifier, retainedShard)
	require.Equal(t, before, after)
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
		return len(writer.getCalls()) == 1 && stageFileCount(t, cfg.StagingDir) == 2
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, "10", writer.getCalls()[0].props[snapshotCommitBarrierTsKey])
	require.NotContains(t, writer.getCalls()[0].props, "ticdc.checkpoint-ts")

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

func TestAddCheckpointTsDrainsPendingCheckpointWhenCommandChannelFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-test"), cfg, writer)
	s.SetTableSchemaStore(nil)
	s.inputCh = make(chan sinkCommand, 1)
	s.inputCh <- sinkCommand{}

	identifier := []string{"test", "orders_cdc"}
	require.NoError(t, s.stage.Write(ctx, s.changefeedID.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))

	s.AddCheckpointTs(10)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 1 && stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-errCh)
}

func TestSinkRecordsIcebergStagedBacklogMetrics(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-metrics")
	s := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})
	defer s.closeMetrics()

	stagedFiles := []stagedFile{
		{sizeBytes: 120, batch: stagedBatch{Rows: []map[string]any{{"id": 1}, {"id": 2}}, CreatedAt: time.Now().Add(-2 * time.Second)}},
		{sizeBytes: 80, batch: stagedBatch{Rows: []map[string]any{{"id": 3}}, CreatedAt: time.Now().Add(-1 * time.Second)}},
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
	require.Greater(t, testutil.ToFloat64(
		metrics.IcebergStagedOldestAgeGauge.WithLabelValues("default", "iceberg-metrics", "pending")), float64(0))
	require.Greater(t, testutil.ToFloat64(
		metrics.IcebergStagedOldestAgeGauge.WithLabelValues("default", "iceberg-metrics", "eligible")), float64(0))
	require.Equal(t, float64(200), testutil.ToFloat64(
		metrics.IcebergStagedBytesGauge.WithLabelValues("default", "iceberg-metrics", "pending")))
	require.Equal(t, float64(120), testutil.ToFloat64(
		metrics.IcebergStagedBytesGauge.WithLabelValues("default", "iceberg-metrics", "eligible")))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.IcebergStagingBackendInfoGauge.WithLabelValues("default", "iceberg-metrics", "local_json")))
}

func TestSinkRecordsIcebergCommitMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedID := common.NewChangefeedID4Test("default", "iceberg-commit-metrics")
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1024)
	stage := newStageStore(cfg.StagingDir)
	require.NoError(t, stage.Write(ctx, changefeedID.String(), []string{"test", "orders_cdc"}, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))

	s := newSink(ctx, changefeedID, cfg, writer)
	defer s.closeMetrics()
	s.SetTableSchemaStore(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddCheckpointTs(20)
	require.Eventually(t, func() bool {
		return len(writer.getCalls()) == 1 && stageFileCount(t, cfg.StagingDir) == 0
	}, 3*time.Second, 10*time.Millisecond)

	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.IcebergCommittedBatchesCounter.WithLabelValues("default", "iceberg-commit-metrics")))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.IcebergCommittedRowsCounter.WithLabelValues("default", "iceberg-commit-metrics")))
	require.Equal(t, float64(10), testutil.ToFloat64(
		metrics.IcebergCommitBarrierLagGauge.WithLabelValues("default", "iceberg-commit-metrics")))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.IcebergCommittedLedgerEntriesGauge.WithLabelValues("default", "iceberg-commit-metrics")))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.IcebergCommittedLedgerWritesCounter.WithLabelValues("default", "iceberg-commit-metrics", "success")))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.IcebergCommittedLedgerLookupsCounter.WithLabelValues("default", "iceberg-commit-metrics", "success")))

	cancel()
	require.NoError(t, <-errCh)
}

func TestCommitterRecordsNegativeLagMetric(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-negative-lag")
	s := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})
	defer s.closeMetrics()

	s.recordCommitSuccess(0, 0, 5, 10)

	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.IcebergNegativeCommitBarrierLagCounter.WithLabelValues("default", "iceberg-negative-lag")))
	require.Equal(t, float64(0), testutil.ToFloat64(
		metrics.IcebergCommitBarrierLagGauge.WithLabelValues("default", "iceberg-negative-lag")))
}

func TestCommitterRetriesIcebergCommitFailedInSameDrain(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-cas-retry")
	writer := &flakyCommitWriter{failures: 1}
	s := newSink(ctx, changefeedID, cfg, writer)
	s.SetTableSchemaStore(nil)

	identifier := []string{"test", "orders_cdc"}
	require.NoError(t, s.stage.Write(ctx, changefeedID.String(), identifier, []map[string]any{
		{"_op": "I", "_commit_ts": int64(10), "_table_id": int64(101), "data": map[string]any{"id": int64(1)}},
	}, 10))

	require.NoError(t, s.drainStaged(ctx, 10))

	require.Len(t, writer.getCalls(), 1)
	require.Equal(t, 0, stageFileCount(t, cfg.StagingDir))
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

func TestWriterClaimsTargetOwnerBeforePostFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := newSinkTestConfig(t, 1)
	stage := newStageStore(cfg.StagingDir)
	identifier := []string{"test", "orders_cdc"}
	require.NoError(t, stage.ClaimTargetOwner(ctx, "default/left", identifier))

	writer := &recordingAppendWriter{}
	right := common.NewChangefeedID4Test("default", "right")
	s := newSink(ctx, right, cfg, writer)

	tableInfo := newPayloadTestTableInfo()
	tableInfo.TableName = common.TableName{Schema: "test", Table: "orders", TableID: 101}
	event := newSinkTestInsertEvent(tableInfo, 10, int64(1), "blocked")
	flushed := make(chan struct{})
	event.AddPostFlushFunc(func() { close(flushed) })

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	s.AddDMLEvent(event)
	select {
	case <-flushed:
		t.Fatal("event was post-flushed before target owner claim succeeded")
	case err := <-errCh:
		require.Error(t, err)
		require.Equal(t, errIcebergTargetOwnerConflict, cerror.Cause(err))
		require.Zero(t, stageFileCount(t, cfg.StagingDir))
	case <-time.After(3 * time.Second):
		t.Fatal("expected target owner conflict before PostFlush")
	}
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

func TestClaimTargetOwnerRevalidatesCachedWarehouseMarker(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	cfg.TiCDCClusterID = "cdc-a"
	cfg.UpstreamID = 1001
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-owner-refresh")
	s := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})
	identifier := []string{"test", "orders_cdc"}

	require.NoError(t, s.claimTargetOwner(ctx, identifier))
	require.NoError(t, icebergcfg.CleanupTargetOwnerClaims(ctx, cfg.Warehouse, s.ownerID))
	require.NoError(t, s.claimTargetOwner(ctx, identifier))

	err := icebergcfg.ClaimTargetOwner(ctx, cfg.Warehouse,
		icebergcfg.NewTargetOwnerClaim("cdc-b", 2002, "default/other", identifier))
	require.Error(t, err)
	require.True(t, cerror.Is(err, icebergcfg.ErrTargetOwnerConflict))
}

func TestClaimTargetOwnerRunsTakeoverReconciliationOnce(t *testing.T) {
	ctx := context.Background()
	cfg := newSinkTestConfig(t, 1024)
	cfg.TiCDCClusterID = "cdc-a"
	cfg.UpstreamID = 1001
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-reconcile")
	writer := &recordingReconcileWriter{}
	s := newSink(ctx, changefeedID, cfg, writer)
	identifier := []string{"test", "orders_cdc"}

	require.NoError(t, s.claimTargetOwner(ctx, identifier))
	require.NoError(t, s.claimTargetOwner(ctx, identifier))

	calls := writer.getReconcileCalls()
	require.Len(t, calls, 1)
	require.Equal(t, identifier, calls[0])
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
		return len(writer.getCalls()) == 1 && stageFileCount(t, cfg.StagingDir) == 0
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

func TestWriteBlockEventRejectsLiveCreateDDL(t *testing.T) {
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

	err := s.WriteBlockEvent(ddl)
	require.Error(t, err)
	require.Contains(t, err.Error(), "iceberg sink does not support block event")
	require.False(t, flushed)
}

func TestWriteBlockEventAllowsBootstrapCreateDDL(t *testing.T) {
	ctx := context.Background()
	writer := &recordingAppendWriter{}
	cfg := newSinkTestConfig(t, 1)
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-test"), cfg, writer)

	ddl := &commonEvent.DDLEvent{
		Type:        byte(model.ActionCreateTable),
		Query:       "create table orders(id bigint primary key)",
		FinishedTs:  42,
		IsBootstrap: true,
	}
	flushed := false
	ddl.AddPostFlushFunc(func() { flushed = true })

	require.NoError(t, s.WriteBlockEvent(ddl))
	require.True(t, flushed)
}

func TestWriteBlockEventAppliesSupportedColumnDDL(t *testing.T) {
	ctx := context.Background()
	writer := &recordingSchemaAppendWriter{}
	cfg := newSinkTestConfig(t, 1)
	cfg.TiCDCClusterID = "cdc-a"
	cfg.UpstreamID = 1001
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-ddl")
	s := newSink(ctx, changefeedID, cfg, writer)
	tableInfo := newRichPayloadTestTableInfo()

	ddl := &commonEvent.DDLEvent{
		Type:       byte(model.ActionAddColumn),
		Query:      "alter table app.rich_types add column amount decimal(20, 4)",
		SchemaName: "app",
		TableName:  "rich_types",
		TableInfo:  tableInfo,
		FinishedTs: 42,
	}
	flushed := false
	ddl.AddPostFlushFunc(func() { flushed = true })

	require.NoError(t, s.WriteBlockEvent(ddl))
	require.True(t, flushed)
	calls := writer.getSchemaCalls()
	require.Len(t, calls, 1)
	require.Equal(t, []string{"app", "rich_types_cdc"}, calls[0].identifier)
	require.Same(t, tableInfo, calls[0].tableInfo)
	require.Equal(t, model.ActionAddColumn, calls[0].ddlType)
	require.Equal(t, s.ownerID, calls[0].ownerID)
	require.Equal(t, changefeedID.String(), calls[0].changefeed)
	require.Equal(t, "cdc-a", calls[0].cdcClusterID)
	require.Equal(t, "1001", calls[0].upstreamID)
}

func TestWriteBlockEventTreatsIndexDDLAsMetadataOnly(t *testing.T) {
	ctx := context.Background()
	writer := &recordingSchemaAppendWriter{}
	cfg := newSinkTestConfig(t, 1)
	s := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-ddl-index"), cfg, writer)

	ddl := &commonEvent.DDLEvent{
		Type:       byte(model.ActionAddIndex),
		Query:      "alter table app.rich_types add index idx_amount(amount)",
		SchemaName: "app",
		TableName:  "rich_types",
		FinishedTs: 42,
	}
	flushed := false
	ddl.AddPostFlushFunc(func() { flushed = true })

	require.NoError(t, s.WriteBlockEvent(ddl))
	require.True(t, flushed)
	require.Empty(t, writer.getSchemaCalls())
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

func rowLedgerFileCount(t *testing.T, root string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		rel, relErr := filepath.Rel(root, path)
		require.NoError(t, relErr)
		if d == nil || d.IsDir() || !strings.Contains(filepath.ToSlash(rel), "/.committed-rows/") {
			return nil
		}
		count++
		return nil
	})
	require.NoError(t, err)
	return count
}

func rowIndexFileCount(t *testing.T, root string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		rel, relErr := filepath.Rel(root, path)
		require.NoError(t, relErr)
		if d == nil || d.IsDir() || !strings.Contains(filepath.ToSlash(rel), "/.committed-row-index/") {
			return nil
		}
		count++
		return nil
	})
	require.NoError(t, err)
	return count
}

func deleteRowIndexShardForTest(t *testing.T, root string, changefeed string, identifier []string, shard string) {
	t.Helper()
	db, err := openCommittedRowIndexDB(committedRowIndexPath(root, changefeed, identifier))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte(shard)); err != nil && err != bolt.ErrBucketNotFound {
			return err
		}
		return nil
	}))
}

func requireRowIndexShardExists(t *testing.T, root string, changefeed string, identifier []string, shard string) {
	t.Helper()
	db, err := openCommittedRowIndexDB(committedRowIndexPath(root, changefeed, identifier))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		require.NotNil(t, tx.Bucket([]byte(shard)))
		return nil
	}))
}

func rowIndexShardHashes(t *testing.T, root string, changefeed string, identifier []string, shard string) []string {
	t.Helper()
	db, err := openCommittedRowIndexDB(committedRowIndexPath(root, changefeed, identifier))
	require.NoError(t, err)
	defer db.Close()

	hashes := make([]string, 0)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(shard))
		require.NotNil(t, bucket)
		return bucket.ForEach(func(key []byte, _ []byte) error {
			hashes = append(hashes, string(key))
			return nil
		})
	}))
	return hashes
}

func rowIDInIndexShard(prefix string, shard string) string {
	for i := 0; ; i++ {
		rowID := fmt.Sprintf("%s-%06d", prefix, i)
		if committedRowIndexShard(rowID) == shard {
			return rowID
		}
	}
}

func rowIDOutsideIndexShard(prefix string, shard string) string {
	for i := 0; ; i++ {
		rowID := fmt.Sprintf("%s-%06d", prefix, i)
		if committedRowIndexShard(rowID) != shard {
			return rowID
		}
	}
}

func newSinkTestInsertEvent(tableInfo *common.TableInfo, commitTs uint64, id int64, name string) *commonEvent.DMLEvent {
	event := newPayloadTestEvent(tableInfo, commitTs, commitTs)
	event.RowTypes = []common.RowType{common.RowTypeInsert}
	event.Length = 1
	event.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	appendPayloadTestRow(event.Rows, id, name)
	return event
}
