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
	"github.com/pingcap/ticdc/pkg/metrics"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type appendWriter interface {
	AppendRows(
		ctx context.Context,
		identifier []string,
		rows []map[string]any,
		tableSchema *stagedTableSchema,
		snapshotProps iceberggo.Properties,
	) error
	CommittedBatches(ctx context.Context, identifier []string) (map[string]struct{}, error)
}

type sink struct {
	changefeedID common.ChangeFeedID
	cfg          *icebergcfg.Config
	ownerID      string
	isNormal     *atomic.Bool

	ctx     context.Context
	inputCh chan sinkCommand

	stage              *stageStore
	isCommitter        *atomic.Bool
	latestCheckpointTs *atomic.Uint64

	writerMu       sync.Mutex
	writer         appendWriter
	injectedWriter bool

	ownerClaimsMu sync.Mutex
	ownerClaims   map[string]struct{}
}

type sinkCommand struct {
	dml          *commonEvent.DMLEvent
	checkpointTs uint64
}

type tableBuffer struct {
	identifier  []string
	tableInfo   *common.TableInfo
	rows        []map[string]any
	events      []*commonEvent.DMLEvent
	maxCommitTs uint64
}

type stagedDrainGroup struct {
	identifier  []string
	tableSchema *stagedTableSchema
	files       []stagedFile
	rows        []map[string]any
	batchIDs    []string
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

const (
	snapshotBatchIDKey         = "ticdc.batch-id"
	snapshotBatchIDsKey        = "ticdc.batch-ids"
	snapshotBatchCountKey      = "ticdc.batch-count"
	snapshotChangefeedKey      = "ticdc.changefeed"
	snapshotOwnerIDKey         = "ticdc.owner-id"
	snapshotCDCClusterIDKey    = "ticdc.cdc-cluster-id"
	snapshotUpstreamIDKey      = "ticdc.upstream-id"
	snapshotCommitBarrierTsKey = "ticdc.commit-barrier-ts"
	tableOwnerKey              = "ticdc.owner-changefeed"
	tableOwnerIDKey            = snapshotOwnerIDKey
	tableCDCClusterIDKey       = snapshotCDCClusterIDKey
	tableUpstreamIDKey         = snapshotUpstreamIDKey

	maxStagedFilesPerCommit = 100
	maxRowsPerCommit        = 5000
)

var errIcebergTargetOwnerConflict = icebergcfg.ErrTargetOwnerConflict

// Verify validates iceberg sink configuration. Catalog/table existence is
// checked lazily by the running sink so changefeed creation does not depend on
// operator bootstrap order.
func Verify(_ context.Context, _ common.ChangeFeedID, sinkURI *url.URL, _ *config.SinkConfig) error {
	_, err := icebergcfg.ParseConfig(sinkURI)
	return err
}

// New creates an Iceberg sink.
func New(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	sinkURI *url.URL,
	_ *config.SinkConfig,
	_ bool,
	upstreamID uint64,
) (*sink, error) {
	cfg, err := icebergcfg.ParseConfig(sinkURI)
	if err != nil {
		return nil, err
	}
	cfg.TiCDCClusterID = config.GetGlobalServerConfig().ClusterID
	cfg.UpstreamID = upstreamID
	return newSink(ctx, changefeedID, cfg, nil), nil
}

func newSink(ctx context.Context, changefeedID common.ChangeFeedID, cfg *icebergcfg.Config, writer appendWriter) *sink {
	if cfg.TiCDCClusterID == "" {
		cfg.TiCDCClusterID = config.GetGlobalServerConfig().ClusterID
	}
	ownerID := icebergcfg.TargetOwnerID(cfg.TiCDCClusterID, cfg.UpstreamID, changefeedID.String())
	return &sink{
		changefeedID:       changefeedID,
		cfg:                cfg,
		ownerID:            ownerID,
		isNormal:           atomic.NewBool(true),
		ctx:                ctx,
		inputCh:            make(chan sinkCommand, 4096),
		stage:              newStageStore(cfg.StagingDir),
		isCommitter:        atomic.NewBool(false),
		latestCheckpointTs: atomic.NewUint64(0),
		writer:             writer,
		injectedWriter:     writer != nil,
		ownerClaims:        make(map[string]struct{}),
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
	switch event.GetType() {
	case commonEvent.TypeSyncPointEvent:
	case commonEvent.TypeDDLEvent:
		ddl, ok := event.(*commonEvent.DDLEvent)
		if !ok || !isIgnorableIcebergDDL(ddl) {
			return errors.Errorf("iceberg sink does not support block event %s at commit ts %d",
				commonEvent.TypeToString(event.GetType()), event.GetCommitTs())
		}
	default:
		return errors.Errorf("iceberg sink does not support block event %s at commit ts %d",
			commonEvent.TypeToString(event.GetType()), event.GetCommitTs())
	}
	event.PostFlush()
	return nil
}

func isIgnorableIcebergDDL(event *commonEvent.DDLEvent) bool {
	return event.IsBootstrap || event.NotSync
}

func (s *sink) AddCheckpointTs(ts uint64) {
	if ts != 0 {
		s.latestCheckpointTs.Store(ts)
	}
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

func (s *sink) Close(removeChangefeed bool) {
	s.isNormal.Store(false)
	s.closeStageMetrics()
	if !removeChangefeed {
		return
	}
	s.closeTargetOwnerConflictMetric()
	if err := s.stage.DeleteChangefeed(s.changefeedID.String()); err != nil {
		s.recordCleanupFailure("changefeed_staging")
		log.Warn("close iceberg sink, remove changefeed staging meet error",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Error(err))
	}
	if err := icebergcfg.CleanupTargetOwnerClaims(context.Background(), s.cfg.Warehouse, s.ownerID); err != nil {
		s.recordCleanupFailure("owner_marker")
		log.Warn("close iceberg sink, remove warehouse target owner claim meet error",
			zap.String("changefeed", s.changefeedID.String()),
			zap.String("ownerID", s.ownerID),
			zap.String("warehouse", s.cfg.Warehouse),
			zap.Error(err))
	}
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

func (s *sink) resetWriter() {
	if s.injectedWriter {
		return
	}
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	s.writer = nil
}

func (s *sink) drainStaged(ctx context.Context, checkpointTs uint64) error {
	if checkpointTs == 0 {
		return nil
	}

	stagedFiles, err := s.stage.List(ctx, s.changefeedID.String())
	if err != nil {
		return errors.Trace(err)
	}
	s.recordStageMetrics(stagedFiles, eligibleStagedFiles(stagedFiles, checkpointTs))

	eligible := stagedFiles[:0]
	for _, staged := range stagedFiles {
		if staged.batch.MaxCommitTs > checkpointTs {
			continue
		}
		eligible = append(eligible, staged)
	}
	if len(eligible) == 0 {
		return nil
	}
	if err := s.claimTargetOwners(ctx, eligible); err != nil {
		return errors.Trace(err)
	}

	writer, err := s.getWriter(ctx)
	if err != nil {
		s.recordAppendFailure("writer_init")
		log.Warn("iceberg committer will retry after writer initialization failure",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Error(err))
		s.resetWriter()
		return nil
	}

	currentGroups := make(map[string]*stagedDrainGroup)
	rowIDsByIdentifier := make(map[string]map[string]struct{})
	committedByIdentifier := make(map[string]map[string]struct{})
	groups := make([]*stagedDrainGroup, 0)
	for _, staged := range eligible {
		rowIDs, err := stagedRowIDs(staged.batch.Identifier, staged.batch.Rows, staged.batch.RowIDs)
		if err != nil {
			return errors.Trace(err)
		}
		staged.batch.RowIDs = rowIDs
		if staged.batch.BatchID == "" {
			batchID, err := stagedBatchID(staged.batch.Identifier, staged.batch.Rows, rowIDs, staged.batch.MaxCommitTs)
			if err != nil {
				return errors.Trace(err)
			}
			staged.batch.BatchID = batchID
		}
		key := identifierKey(staged.batch.Identifier)
		seenRowIDs := rowIDsByIdentifier[key]
		if seenRowIDs == nil {
			seenRowIDs = make(map[string]struct{}, len(staged.batch.Rows))
			rowIDsByIdentifier[key] = seenRowIDs
		}
		committedBatches, ok := committedByIdentifier[key]
		if !ok {
			var err error
			committedBatches, err = writer.CommittedBatches(ctx, staged.batch.Identifier)
			if err != nil {
				s.recordAppendFailure("committed_batch_lookup")
				log.Warn("iceberg committer will retry after committed batch lookup failure",
					zap.String("changefeed", s.changefeedID.String()),
					zap.Strings("identifier", staged.batch.Identifier),
					zap.Error(err))
				s.resetWriter()
				return nil
			}
			committedByIdentifier[key] = committedBatches
		}
		if _, committed := committedBatches[staged.batch.BatchID]; committed {
			rememberRowIDs(seenRowIDs, rowIDs)
			if stagedBatchSafeToDelete(staged.batch, checkpointTs) {
				if err := s.stage.Delete(staged.path); err != nil {
					return errors.Trace(err)
				}
			}
			log.Info("iceberg committer skipped already committed staged batch",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", staged.batch.Identifier),
				zap.String("batchID", staged.batch.BatchID),
				zap.Int("rows", len(staged.batch.Rows)),
				zap.Uint64("maxCommitTs", staged.batch.MaxCommitTs),
				zap.Bool("deleted", stagedBatchSafeToDelete(staged.batch, checkpointTs)))
			continue
		}

		uniqueRows, err := uniqueStagedRows(staged.batch.Rows, rowIDs, seenRowIDs)
		if err != nil {
			return errors.Trace(err)
		}
		group := currentGroups[key]
		if group == nil ||
			len(group.batchIDs) >= maxStagedFilesPerCommit ||
			len(uniqueRows) > 0 && len(group.rows)+len(uniqueRows) > maxRowsPerCommit ||
			!sameStagedTableSchema(group.tableSchema, staged.batch.TableSchema) {
			group = &stagedDrainGroup{
				identifier:  append([]string(nil), staged.batch.Identifier...),
				tableSchema: staged.batch.TableSchema,
			}
			currentGroups[key] = group
			groups = append(groups, group)
		}
		if group.tableSchema == nil {
			group.tableSchema = staged.batch.TableSchema
		}
		group.files = append(group.files, staged)
		group.rows = append(group.rows, uniqueRows...)
		group.batchIDs = append(group.batchIDs, staged.batch.BatchID)
		if staged.batch.MaxCommitTs > group.maxCommitTs {
			group.maxCommitTs = staged.batch.MaxCommitTs
		}
	}

	for _, group := range groups {
		if len(group.rows) == 0 {
			if err := s.deleteStagedFiles(group.files); err != nil {
				return errors.Trace(err)
			}
			log.Info("iceberg committer deleted duplicate staged rows",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", group.identifier),
				zap.Int("files", len(group.files)),
				zap.Strings("batchIDs", group.batchIDs),
				zap.Uint64("maxCommitTs", group.maxCommitTs),
				zap.Uint64("checkpointTs", checkpointTs))
			continue
		}

		props := iceberggo.Properties{
			snapshotChangefeedKey:      s.changefeedID.String(),
			snapshotOwnerIDKey:         s.ownerID,
			snapshotCDCClusterIDKey:    s.cfg.TiCDCClusterID,
			snapshotUpstreamIDKey:      strconv.FormatUint(s.cfg.UpstreamID, 10),
			"ticdc.commit-ts":          strconv.FormatUint(group.maxCommitTs, 10),
			snapshotBatchIDsKey:        strings.Join(group.batchIDs, ","),
			snapshotBatchCountKey:      strconv.Itoa(len(group.batchIDs)),
			snapshotCommitBarrierTsKey: strconv.FormatUint(checkpointTs, 10),
		}
		if len(group.batchIDs) == 1 {
			props[snapshotBatchIDKey] = group.batchIDs[0]
		}
		appendStart := time.Now()
		if err := writer.AppendRows(ctx, group.identifier, group.rows, group.tableSchema, props); err != nil {
			s.recordCommitDuration("error", time.Since(appendStart))
			s.recordAppendFailure(appendFailureReason(err))
			if errors.Is(err, errIcebergTargetOwnerConflict) || errors.Cause(err) == errIcebergTargetOwnerConflict {
				s.recordTargetOwnerConflict(err)
				return errors.Trace(err)
			}
			committed, checkErr := allBatchesCommitted(ctx, writer, group.identifier, group.batchIDs)
			if checkErr == nil && committed {
				if err := s.deleteStagedFiles(group.files); err != nil {
					return errors.Trace(err)
				}
				continue
			}
			if checkErr != nil {
				s.recordAppendFailure("commit_state_lookup")
			}
			log.Warn("iceberg committer will retry staged append after failure",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", group.identifier),
				zap.Int("rows", len(group.rows)),
				zap.Strings("batchIDs", group.batchIDs),
				zap.Error(err))
			s.resetWriter()
			return nil
		}
		s.recordCommitDuration("success", time.Since(appendStart))
		s.recordCommitSuccess(len(group.batchIDs), checkpointTs, group.maxCommitTs)
		if err := runIcebergFaultHook(icebergFaultAfterAppendBeforeStageDelete); err != nil {
			return errors.Trace(err)
		}
		failpoint.Inject("IcebergSinkErrorAfterAppendBeforeStageDelete", func() {
			log.Warn("inject IcebergSinkErrorAfterAppendBeforeStageDelete",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", group.identifier),
				zap.Int("rows", len(group.rows)),
				zap.Uint64("maxCommitTs", group.maxCommitTs))
			failpoint.Return(errors.New("injected iceberg error after append before stage delete"))
		})
		failpoint.Inject("IcebergSinkExitAfterAppendBeforeStageDelete", func() {
			log.Warn("inject IcebergSinkExitAfterAppendBeforeStageDelete",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", group.identifier),
				zap.Int("rows", len(group.rows)),
				zap.Uint64("maxCommitTs", group.maxCommitTs))
			os.Exit(78)
		})
		if stagedGroupSafeToDelete(group, checkpointTs) {
			if err := s.deleteStagedFiles(group.files); err != nil {
				return errors.Trace(err)
			}
		}
		log.Info("iceberg committer appended staged rows",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Strings("identifier", group.identifier),
			zap.Int("rows", len(group.rows)),
			zap.Int("batches", len(group.batchIDs)),
			zap.Uint64("maxCommitTs", group.maxCommitTs),
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Bool("deleted", stagedGroupSafeToDelete(group, checkpointTs)))
	}
	if err := s.refreshStageMetrics(ctx, checkpointTs); err != nil {
		log.Warn("failed to refresh iceberg staged metrics",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Error(err))
	}
	return nil
}

func (s *sink) claimTargetOwners(ctx context.Context, stagedFiles []stagedFile) error {
	claimed := make(map[string]struct{})
	for _, staged := range stagedFiles {
		key := identifierKey(staged.batch.Identifier)
		if _, ok := claimed[key]; ok {
			continue
		}
		if err := s.claimTargetOwner(ctx, staged.batch.Identifier); err != nil {
			return errors.Trace(err)
		}
		claimed[key] = struct{}{}
	}
	return nil
}

func (s *sink) claimTargetOwner(ctx context.Context, identifier []string) error {
	key := identifierKey(identifier)
	s.ownerClaimsMu.Lock()
	_, cached := s.ownerClaims[key]
	s.ownerClaimsMu.Unlock()
	if cached {
		return nil
	}
	if err := s.stage.ClaimTargetOwner(ctx, s.changefeedID.String(), identifier); err != nil {
		s.recordTargetOwnerConflict(err)
		return errors.Trace(err)
	}
	claim := icebergcfg.NewTargetOwnerClaim(
		s.cfg.TiCDCClusterID,
		s.cfg.UpstreamID,
		s.changefeedID.String(),
		identifier)
	if err := icebergcfg.ClaimTargetOwner(ctx, s.cfg.Warehouse, claim); err != nil {
		s.recordTargetOwnerConflict(err)
		return errors.Trace(err)
	}
	s.ownerClaimsMu.Lock()
	s.ownerClaims[key] = struct{}{}
	s.ownerClaimsMu.Unlock()
	return nil
}

func (s *sink) recordStageMetrics(stagedFiles []stagedFile, eligible []stagedFile) {
	keyspace, changefeed := s.changefeedID.Keyspace(), s.changefeedID.Name()
	pendingRows := stagedRows(stagedFiles)
	eligibleRows := stagedRows(eligible)
	metrics.IcebergStagedFilesGauge.WithLabelValues(keyspace, changefeed, "pending").Set(float64(len(stagedFiles)))
	metrics.IcebergStagedFilesGauge.WithLabelValues(keyspace, changefeed, "eligible").Set(float64(len(eligible)))
	metrics.IcebergStagedRowsGauge.WithLabelValues(keyspace, changefeed, "pending").Set(float64(pendingRows))
	metrics.IcebergStagedRowsGauge.WithLabelValues(keyspace, changefeed, "eligible").Set(float64(eligibleRows))
	metrics.IcebergStagedOldestAgeGauge.WithLabelValues(keyspace, changefeed, "pending").Set(stagedOldestAgeSeconds(stagedFiles))
	metrics.IcebergStagedOldestAgeGauge.WithLabelValues(keyspace, changefeed, "eligible").Set(stagedOldestAgeSeconds(eligible))
}

func (s *sink) refreshStageMetrics(ctx context.Context, checkpointTs uint64) error {
	stagedFiles, err := s.stage.List(ctx, s.changefeedID.String())
	if err != nil {
		return errors.Trace(err)
	}
	s.recordStageMetrics(stagedFiles, eligibleStagedFiles(stagedFiles, checkpointTs))
	return nil
}

func (s *sink) recordTargetOwnerConflict(err error) {
	if !errors.Is(err, errIcebergTargetOwnerConflict) && errors.Cause(err) != errIcebergTargetOwnerConflict {
		return
	}
	metrics.IcebergTargetOwnerConflictCounter.WithLabelValues(
		s.changefeedID.Keyspace(), s.changefeedID.Name()).Inc()
}

func (s *sink) recordCommitDuration(result string, duration time.Duration) {
	metrics.IcebergCommitDurationHistogram.WithLabelValues(
		s.changefeedID.Keyspace(), s.changefeedID.Name(), result).Observe(duration.Seconds())
}

func (s *sink) recordCommitSuccess(batchCount int, checkpointTs uint64, maxCommitTs uint64) {
	keyspace, changefeed := s.changefeedID.Keyspace(), s.changefeedID.Name()
	metrics.IcebergCommittedBatchesCounter.WithLabelValues(keyspace, changefeed).Add(float64(batchCount))
	if checkpointTs >= maxCommitTs {
		metrics.IcebergCommitBarrierLagGauge.WithLabelValues(keyspace, changefeed).Set(float64(checkpointTs - maxCommitTs))
		return
	}
	metrics.IcebergCommitBarrierLagGauge.WithLabelValues(keyspace, changefeed).Set(0)
}

func (s *sink) recordAppendFailure(reason string) {
	metrics.IcebergAppendFailureCounter.WithLabelValues(
		s.changefeedID.Keyspace(), s.changefeedID.Name(), reason).Inc()
}

func appendFailureReason(err error) string {
	if errors.Is(err, errIcebergTargetOwnerConflict) || errors.Cause(err) == errIcebergTargetOwnerConflict {
		return "owner_conflict"
	}
	return "append"
}

func (s *sink) recordCleanupFailure(reason string) {
	metrics.IcebergCleanupFailureCounter.WithLabelValues(
		s.changefeedID.Keyspace(), s.changefeedID.Name(), reason).Inc()
}

func (s *sink) closeMetrics() {
	s.closeStageMetrics()
	s.closeTargetOwnerConflictMetric()
}

func (s *sink) closeStageMetrics() {
	keyspace, changefeed := s.changefeedID.Keyspace(), s.changefeedID.Name()
	metrics.IcebergStagedFilesGauge.DeleteLabelValues(keyspace, changefeed, "pending")
	metrics.IcebergStagedFilesGauge.DeleteLabelValues(keyspace, changefeed, "eligible")
	metrics.IcebergStagedRowsGauge.DeleteLabelValues(keyspace, changefeed, "pending")
	metrics.IcebergStagedRowsGauge.DeleteLabelValues(keyspace, changefeed, "eligible")
	metrics.IcebergStagedOldestAgeGauge.DeleteLabelValues(keyspace, changefeed, "pending")
	metrics.IcebergStagedOldestAgeGauge.DeleteLabelValues(keyspace, changefeed, "eligible")
	metrics.IcebergCommitBarrierLagGauge.DeleteLabelValues(keyspace, changefeed)
}

func (s *sink) closeTargetOwnerConflictMetric() {
	keyspace, changefeed := s.changefeedID.Keyspace(), s.changefeedID.Name()
	metrics.IcebergTargetOwnerConflictCounter.DeleteLabelValues(keyspace, changefeed)
}

func eligibleStagedFiles(stagedFiles []stagedFile, checkpointTs uint64) []stagedFile {
	if checkpointTs == 0 {
		return nil
	}
	eligible := make([]stagedFile, 0, len(stagedFiles))
	for _, staged := range stagedFiles {
		if staged.batch.MaxCommitTs <= checkpointTs {
			eligible = append(eligible, staged)
		}
	}
	return eligible
}

func stagedRows(stagedFiles []stagedFile) int {
	rows := 0
	for _, staged := range stagedFiles {
		rows += len(staged.batch.Rows)
	}
	return rows
}

func stagedOldestAgeSeconds(stagedFiles []stagedFile) float64 {
	var oldest time.Time
	for _, staged := range stagedFiles {
		if staged.batch.CreatedAt.IsZero() {
			continue
		}
		if oldest.IsZero() || staged.batch.CreatedAt.Before(oldest) {
			oldest = staged.batch.CreatedAt
		}
	}
	if oldest.IsZero() {
		return 0
	}
	age := time.Since(oldest)
	if age < 0 {
		return 0
	}
	return age.Seconds()
}

func stagedGroupSafeToDelete(group *stagedDrainGroup, checkpointTs uint64) bool {
	return checkpointTs != 0 && group.maxCommitTs <= checkpointTs
}

func stagedBatchSafeToDelete(batch stagedBatch, checkpointTs uint64) bool {
	return checkpointTs != 0 && batch.MaxCommitTs <= checkpointTs
}

func uniqueStagedRows(
	rows []map[string]any,
	rowIDs []string,
	seenRowIDs map[string]struct{},
) ([]map[string]any, error) {
	if len(rowIDs) != len(rows) {
		return nil, errors.Errorf("iceberg staged row id count %d does not match row count %d",
			len(rowIDs), len(rows))
	}
	uniqueRows := make([]map[string]any, 0, len(rows))
	for i, rowID := range rowIDs {
		if _, ok := seenRowIDs[rowID]; ok {
			continue
		}
		seenRowIDs[rowID] = struct{}{}
		uniqueRows = append(uniqueRows, rows[i])
	}
	return uniqueRows, nil
}

func rememberRowIDs(seenRowIDs map[string]struct{}, rowIDs []string) {
	for _, rowID := range rowIDs {
		seenRowIDs[rowID] = struct{}{}
	}
}

func allBatchesCommitted(ctx context.Context, writer appendWriter, identifier []string, batchIDs []string) (bool, error) {
	committedBatches, err := writer.CommittedBatches(ctx, identifier)
	if err != nil {
		return false, errors.Trace(err)
	}
	for _, batchID := range batchIDs {
		if _, committed := committedBatches[batchID]; !committed {
			return false, nil
		}
	}
	return true, nil
}

func (s *sink) deleteStagedFiles(files []stagedFile) error {
	for _, staged := range files {
		if err := s.stage.Delete(staged.path); err != nil {
			s.recordCleanupFailure("staged_file_delete")
			return errors.Trace(err)
		}
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

	if err := s.claimTargetOwner(ctx, buffer.identifier); err != nil {
		return errors.Trace(err)
	}
	if err := s.stage.Write(ctx, s.changefeedID.String(), buffer.identifier, buffer.rows, buffer.maxCommitTs, buffer.tableInfo); err != nil {
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
		buffer = &tableBuffer{identifier: identifier, tableInfo: event.TableInfo}
		buffers[key] = buffer
	}
	if buffer.tableInfo == nil {
		buffer.tableInfo = event.TableInfo
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

func batchIDsFromProps(props iceberggo.Properties) []string {
	if len(props) == 0 {
		return nil
	}
	if raw := props[snapshotBatchIDsKey]; raw != "" {
		values := strings.Split(raw, ",")
		batchIDs := make([]string, 0, len(values))
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value != "" {
				batchIDs = append(batchIDs, value)
			}
		}
		return batchIDs
	}
	if raw := strings.TrimSpace(props[snapshotBatchIDKey]); raw != "" {
		return []string{raw}
	}
	return nil
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
