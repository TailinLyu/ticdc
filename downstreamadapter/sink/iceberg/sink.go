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
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	iceberggo "github.com/apache/iceberg-go"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type appendWriter interface {
	AppendRows(ctx context.Context, identifier []string, rows []map[string]any, snapshotProps iceberggo.Properties) error
}

type sink struct {
	changefeedID common.ChangeFeedID
	cfg          *icebergcfg.Config
	isNormal     *atomic.Bool

	ctx     context.Context
	inputCh chan sinkCommand

	stage              *stageStore
	isCommitter        *atomic.Bool
	latestCheckpointTs *atomic.Uint64

	writerMu sync.Mutex
	writer   appendWriter
}

type sinkCommand struct {
	dml          *commonEvent.DMLEvent
	checkpointTs uint64
}

type tableBuffer struct {
	identifier  []string
	rows        []map[string]any
	events      []*commonEvent.DMLEvent
	maxCommitTs uint64
}

type icebergFaultHookName int

const (
	icebergFaultAfterStageBeforePostFlush icebergFaultHookName = iota
	icebergFaultAfterAppendBeforeStageDelete
)

var icebergFaultHooks = struct {
	sync.RWMutex
	afterStageBeforePostFlush    func() error
	afterAppendBeforeStageDelete func() error
}{}

// Verify validates iceberg sink configuration. Catalog/table existence is
// checked lazily by the running sink so changefeed creation does not depend on
// operator bootstrap order.
func Verify(_ context.Context, _ common.ChangeFeedID, sinkURI *url.URL, _ *config.SinkConfig) error {
	_, err := icebergcfg.ParseConfig(sinkURI)
	return err
}

// New creates an Iceberg sink.
func New(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, _ *config.SinkConfig, _ bool) (*sink, error) {
	cfg, err := icebergcfg.ParseConfig(sinkURI)
	if err != nil {
		return nil, err
	}
	return newSink(ctx, changefeedID, cfg, nil), nil
}

func newSink(ctx context.Context, changefeedID common.ChangeFeedID, cfg *icebergcfg.Config, writer appendWriter) *sink {
	return &sink{
		changefeedID:       changefeedID,
		cfg:                cfg,
		isNormal:           atomic.NewBool(true),
		ctx:                ctx,
		inputCh:            make(chan sinkCommand, 4096),
		stage:              newStageStore(cfg.StagingDir),
		isCommitter:        atomic.NewBool(false),
		latestCheckpointTs: atomic.NewUint64(0),
		writer:             writer,
	}
}

func (s *sink) SinkType() common.SinkType {
	return common.IcebergSinkType
}

func (s *sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	select {
	case s.inputCh <- sinkCommand{dml: event}:
	case <-s.ctx.Done():
		return
	}
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	event.PostFlush()
	return nil
}

func (s *sink) AddCheckpointTs(ts uint64) {
	select {
	case s.inputCh <- sinkCommand{checkpointTs: ts}:
	case <-s.ctx.Done():
	default:
	}
}

func (s *sink) SetTableSchemaStore(_ *commonEvent.TableSchemaStore) {
	if s.isCommitter.CompareAndSwap(false, true) {
		log.Info("iceberg committer enabled",
			zap.String("changefeed", s.changefeedID.String()),
			zap.String("stagingDir", s.cfg.StagingDir))
	}
}

func (s *sink) Close(bool) {
	s.isNormal.Store(false)
}

func (s *sink) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.CommitInterval)
	defer ticker.Stop()

	buffers := make(map[string]*tableBuffer)
	for {
		select {
		case <-ctx.Done():
			return nil
		case cmd := <-s.inputCh:
			if cmd.dml != nil {
				if err := s.bufferEvent(buffers, cmd.dml); err != nil {
					s.isNormal.Store(false)
					return errors.Trace(err)
				}
				key := identifierKey(s.cfg.TargetIdentifier(cmd.dml.TableInfo.GetSchemaName(), cmd.dml.TableInfo.GetTableName()))
				if len(buffers[key].rows) >= s.cfg.BatchRows {
					if err := s.stageTable(ctx, buffers, key); err != nil {
						s.isNormal.Store(false)
						return errors.Trace(err)
					}
				}
				continue
			}
			if cmd.checkpointTs != 0 {
				s.latestCheckpointTs.Store(cmd.checkpointTs)
			}
			if err := s.stageAll(ctx, buffers); err != nil {
				s.isNormal.Store(false)
				return errors.Trace(err)
			}
			if s.isCommitter.Load() {
				failpoint.Inject("IcebergSinkBlockBeforeDrain", nil)
				if err := s.drainStaged(ctx, cmd.checkpointTs); err != nil {
					s.isNormal.Store(false)
					return errors.Trace(err)
				}
			}
		case <-ticker.C:
			if err := s.stageAll(ctx, buffers); err != nil {
				s.isNormal.Store(false)
				return errors.Trace(err)
			}
			if s.isCommitter.Load() {
				failpoint.Inject("IcebergSinkBlockBeforeDrain", nil)
				if err := s.drainStaged(ctx, s.latestCheckpointTs.Load()); err != nil {
					s.isNormal.Store(false)
					return errors.Trace(err)
				}
			}
		}
	}
}

func (s *sink) getWriter(ctx context.Context) (appendWriter, error) {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()

	if s.writer != nil {
		return s.writer, nil
	}
	writer, err := newIcebergAppender(ctx, s.cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.writer = writer
	return writer, nil
}

func (s *sink) drainStaged(ctx context.Context, checkpointTs uint64) error {
	stagedFiles, err := s.stage.List(ctx, s.changefeedID.String())
	if err != nil {
		return errors.Trace(err)
	}

	eligible := stagedFiles[:0]
	for _, staged := range stagedFiles {
		if checkpointTs != 0 && staged.batch.MaxCommitTs > checkpointTs {
			continue
		}
		eligible = append(eligible, staged)
	}
	if len(eligible) == 0 {
		return nil
	}

	writer, err := s.getWriter(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	for _, staged := range eligible {
		props := iceberggo.Properties{
			"ticdc.changefeed": s.changefeedID.String(),
			"ticdc.commit-ts":  strconv.FormatUint(staged.batch.MaxCommitTs, 10),
		}
		if checkpointTs != 0 {
			props["ticdc.checkpoint-ts"] = strconv.FormatUint(checkpointTs, 10)
		}

		if err := writer.AppendRows(ctx, staged.batch.Identifier, staged.batch.Rows, props); err != nil {
			return errors.Trace(err)
		}
		if err := runIcebergFaultHook(icebergFaultAfterAppendBeforeStageDelete); err != nil {
			return errors.Trace(err)
		}
		failpoint.Inject("IcebergSinkErrorAfterAppendBeforeStageDelete", func() {
			log.Warn("inject IcebergSinkErrorAfterAppendBeforeStageDelete",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", staged.batch.Identifier),
				zap.Int("rows", len(staged.batch.Rows)),
				zap.Uint64("maxCommitTs", staged.batch.MaxCommitTs))
			failpoint.Return(errors.New("injected iceberg error after append before stage delete"))
		})
		failpoint.Inject("IcebergSinkExitAfterAppendBeforeStageDelete", func() {
			log.Warn("inject IcebergSinkExitAfterAppendBeforeStageDelete",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", staged.batch.Identifier),
				zap.Int("rows", len(staged.batch.Rows)),
				zap.Uint64("maxCommitTs", staged.batch.MaxCommitTs))
			os.Exit(78)
		})
		if err := s.stage.Delete(staged.path); err != nil {
			return errors.Trace(err)
		}
		log.Info("iceberg committer appended staged rows",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Strings("identifier", staged.batch.Identifier),
			zap.Int("rows", len(staged.batch.Rows)),
			zap.Uint64("maxCommitTs", staged.batch.MaxCommitTs),
			zap.Uint64("checkpointTs", checkpointTs))
	}
	return nil
}

func (s *sink) stageAll(ctx context.Context, buffers map[string]*tableBuffer) error {
	for key := range buffers {
		if err := s.stageTable(ctx, buffers, key); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *sink) stageTable(ctx context.Context, buffers map[string]*tableBuffer, key string) error {
	buffer := buffers[key]
	if buffer == nil || len(buffer.rows) == 0 {
		return nil
	}

	if err := s.stage.Write(ctx, s.changefeedID.String(), buffer.identifier, buffer.rows, buffer.maxCommitTs); err != nil {
		return errors.Trace(err)
	}
	if err := runIcebergFaultHook(icebergFaultAfterStageBeforePostFlush); err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("IcebergSinkErrorAfterStageBeforePostFlush", func() {
		log.Warn("inject IcebergSinkErrorAfterStageBeforePostFlush",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Strings("identifier", buffer.identifier),
			zap.Int("rows", len(buffer.rows)),
			zap.Uint64("maxCommitTs", buffer.maxCommitTs))
		failpoint.Return(errors.New("injected iceberg error after stage before postflush"))
	})
	failpoint.Inject("IcebergSinkExitAfterStageBeforePostFlush", func() {
		log.Warn("inject IcebergSinkExitAfterStageBeforePostFlush",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Strings("identifier", buffer.identifier),
			zap.Int("rows", len(buffer.rows)),
			zap.Uint64("maxCommitTs", buffer.maxCommitTs))
		os.Exit(77)
	})
	for _, event := range buffer.events {
		event.PostFlush()
	}
	log.Info("iceberg writer staged rows",
		zap.String("changefeed", s.changefeedID.String()),
		zap.Strings("identifier", buffer.identifier),
		zap.Int("rows", len(buffer.rows)),
		zap.Uint64("maxCommitTs", buffer.maxCommitTs),
		zap.Bool("committer", s.isCommitter.Load()))
	delete(buffers, key)
	return nil
}

func (s *sink) bufferEvent(buffers map[string]*tableBuffer, event *commonEvent.DMLEvent) error {
	rows, err := buildPayloadRows(event)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		event.PostFlush()
		return nil
	}

	identifier := s.cfg.TargetIdentifier(event.TableInfo.GetSchemaName(), event.TableInfo.GetTableName())
	key := identifierKey(identifier)
	buffer := buffers[key]
	if buffer == nil {
		buffer = &tableBuffer{identifier: identifier}
		buffers[key] = buffer
	}
	buffer.rows = append(buffer.rows, rows...)
	buffer.events = append(buffer.events, event)
	if event.CommitTs > buffer.maxCommitTs {
		buffer.maxCommitTs = event.CommitTs
	}
	return nil
}

func identifierKey(identifier []string) string {
	return strings.Join(identifier, "\x1f")
}

func runIcebergFaultHook(name icebergFaultHookName) error {
	icebergFaultHooks.RLock()
	var hook func() error
	switch name {
	case icebergFaultAfterStageBeforePostFlush:
		hook = icebergFaultHooks.afterStageBeforePostFlush
	case icebergFaultAfterAppendBeforeStageDelete:
		hook = icebergFaultHooks.afterAppendBeforeStageDelete
	}
	icebergFaultHooks.RUnlock()
	if hook == nil {
		return nil
	}
	return hook()
}
