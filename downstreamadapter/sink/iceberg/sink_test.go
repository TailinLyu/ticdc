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
	"path/filepath"
	"sync"
	"testing"
	"time"

	iceberggo "github.com/apache/iceberg-go"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

type appendCall struct {
	identifier []string
	rows       []map[string]any
	props      iceberggo.Properties
}

type recordingAppendWriter struct {
	mu    sync.Mutex
	calls []appendCall
	err   error
}

func (r *recordingAppendWriter) AppendRows(
	_ context.Context,
	identifier []string,
	rows []map[string]any,
	props iceberggo.Properties,
) error {
	if r.err != nil {
		return r.err
	}
	call := appendCall{
		identifier: append([]string(nil), identifier...),
		rows:       append([]map[string]any(nil), rows...),
		props:      props,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
	return nil
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
	require.Equal(t, 1, stageFileCount(t, cfg.StagingDir))
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

func newSinkTestConfig(t *testing.T, batchRows int) *icebergcfg.Config {
	t.Helper()
	return &icebergcfg.Config{
		CatalogURI:     "http://localhost:8181/",
		Warehouse:      "file:///tmp/iceberg-warehouse",
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
