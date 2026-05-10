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
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	iceberggo "github.com/apache/iceberg-go"
	icebergrest "github.com/apache/iceberg-go/catalog/rest"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/metrics"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"go.etcd.io/etcd/client/v3/concurrency"
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
	CommittedBatches(ctx context.Context, identifier []string, batchIDs []string) (map[string]struct{}, error)
}

type tableSchemaWriter interface {
	ApplyTableSchema(
		ctx context.Context,
		identifier []string,
		tableInfo *common.TableInfo,
		ddlType timodel.ActionType,
		ownerID string,
		changefeed string,
		cdcClusterID string,
		upstreamID string,
	) error
}

type targetTakeoverReconciler interface {
	ReconcileTargetOnTakeover(ctx context.Context, identifier []string, ownerID string, changefeed string) error
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
	checkpointPending  *atomic.Bool

	writerMu       sync.Mutex
	writer         appendWriter
	injectedWriter bool

	ownerClaimsMu sync.Mutex
	ownerClaims   map[string]struct{}
	ownerLeases   map[string]*concurrency.Election

	committedRowIDsByIdentifier map[string]*committedRowIDCache
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
	rowIDs      []string
	maxCommitTs uint64
}

type preparedStagedFile struct {
	file   stagedFile
	rowIDs []string
}

// committedRowIDCache optimizes partial-overlap replays within one committer
// process. Durable correctness comes from retaining staged evidence until later
// staged files for the same target become eligible.
type committedRowIDCache struct {
	set   map[string]struct{}
	order []string
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
	maxCommittedRowIDCache  = maxRowsPerCommit * 4
	maxCommitConflictRetry  = 3
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
		changefeedID:                changefeedID,
		cfg:                         cfg,
		ownerID:                     ownerID,
		isNormal:                    atomic.NewBool(true),
		ctx:                         ctx,
		inputCh:                     make(chan sinkCommand, 4096),
		stage:                       newStageStore(cfg.StagingDir),
		isCommitter:                 atomic.NewBool(false),
		latestCheckpointTs:          atomic.NewUint64(0),
		checkpointPending:           atomic.NewBool(false),
		writer:                      writer,
		injectedWriter:              writer != nil,
		ownerClaims:                 make(map[string]struct{}),
		ownerLeases:                 make(map[string]*concurrency.Election),
		committedRowIDsByIdentifier: make(map[string]*committedRowIDCache),
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
		if !ok {
			return errors.Errorf("iceberg sink does not support block event %s at commit ts %d",
				commonEvent.TypeToString(event.GetType()), event.GetCommitTs())
		}
		if isIgnorableIcebergDDL(ddl) || isMetadataOnlyIcebergDDL(ddl.GetDDLType()) {
			break
		}
		if isSchemaEvolutionIcebergDDL(ddl.GetDDLType()) {
			if err := s.applySchemaEvolutionDDL(ddl); err != nil {
				return errors.Trace(err)
			}
			break
		}
		return errors.Errorf("iceberg sink does not support block event %s at commit ts %d",
			commonEvent.TypeToString(event.GetType()), event.GetCommitTs())
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

func isSchemaEvolutionIcebergDDL(action timodel.ActionType) bool {
	switch action {
	case timodel.ActionAddColumn,
		timodel.ActionAddColumns,
		timodel.ActionDropColumn,
		timodel.ActionDropColumns,
		timodel.ActionModifyColumn,
		timodel.ActionMultiSchemaChange:
		return true
	default:
		return false
	}
}

func isMetadataOnlyIcebergDDL(action timodel.ActionType) bool {
	switch action {
	case timodel.ActionAddIndex,
		timodel.ActionDropIndex,
		timodel.ActionAddPrimaryKey,
		timodel.ActionDropPrimaryKey,
		timodel.ActionRenameIndex,
		timodel.ActionAlterIndexVisibility,
		timodel.ActionSetDefaultValue,
		timodel.ActionModifyTableComment,
		timodel.ActionModifyTableCharsetAndCollate,
		timodel.ActionRebaseAutoID,
		timodel.ActionShardRowID,
		timodel.ActionModifyTableAutoIDCache,
		timodel.ActionRebaseAutoRandomBase,
		timodel.ActionAlterTTLInfo,
		timodel.ActionAlterTTLRemove:
		return true
	default:
		return false
	}
}

func (s *sink) applySchemaEvolutionDDL(ddl *commonEvent.DDLEvent) error {
	if ddl.TableInfo == nil {
		return errors.Errorf("iceberg schema DDL %s at commit ts %d has no table info",
			ddl.GetDDLType().String(), ddl.GetCommitTs())
	}
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	identifier := s.cfg.TargetIdentifier(ddl.TableInfo.GetSchemaName(), ddl.TableInfo.GetTableName())
	if err := s.claimTargetOwner(ctx, identifier); err != nil {
		return errors.Trace(err)
	}
	writer, err := s.getWriter(ctx)
	if err != nil {
		s.recordAppendFailure("writer_init")
		s.resetWriter()
		return errors.Trace(err)
	}
	schemaWriter, ok := writer.(tableSchemaWriter)
	if !ok {
		return errors.Errorf("iceberg writer %T cannot apply schema DDL %s", writer, ddl.GetDDLType().String())
	}
	return schemaWriter.ApplyTableSchema(
		ctx,
		identifier,
		ddl.TableInfo,
		ddl.GetDDLType(),
		s.ownerID,
		s.changefeedID.String(),
		s.cfg.TiCDCClusterID,
		strconv.FormatUint(s.cfg.UpstreamID, 10))
}

func (s *sink) AddCheckpointTs(ts uint64) {
	if ts != 0 {
		s.latestCheckpointTs.Store(ts)
		s.checkpointPending.Store(true)
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
	s.releaseTargetCommitterLeases()
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
					if isContextDoneError(ctx, err) {
						return nil
					}
					s.isNormal.Store(false)
					return errors.Trace(err)
				}
				key := identifierKey(s.cfg.TargetIdentifier(cmd.dml.TableInfo.GetSchemaName(), cmd.dml.TableInfo.GetTableName()))
				if len(buffers[key].rows) >= s.cfg.BatchRows {
					if err := s.stageTable(ctx, buffers, key); err != nil {
						if isContextDoneError(ctx, err) {
							return nil
						}
						s.isNormal.Store(false)
						return errors.Trace(err)
					}
				}
				if s.checkpointPending.Load() {
					if err := s.stageAllAndDrain(ctx, buffers, s.latestCheckpointTs.Load()); err != nil {
						if isContextDoneError(ctx, err) {
							return nil
						}
						s.isNormal.Store(false)
						return errors.Trace(err)
					}
				}
				continue
			}
			if cmd.checkpointTs != 0 {
				s.latestCheckpointTs.Store(cmd.checkpointTs)
			}
			if err := s.stageAllAndDrain(ctx, buffers, cmd.checkpointTs); err != nil {
				if isContextDoneError(ctx, err) {
					return nil
				}
				s.isNormal.Store(false)
				return errors.Trace(err)
			}
		case <-ticker.C:
			if err := s.stageAllAndDrain(ctx, buffers, s.latestCheckpointTs.Load()); err != nil {
				if isContextDoneError(ctx, err) {
					return nil
				}
				s.isNormal.Store(false)
				return errors.Trace(err)
			}
		}
	}
}

func isContextDoneError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	cause := errors.Cause(err)
	return cause == context.Canceled || cause == context.DeadlineExceeded || cause == ctx.Err()
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
	latestStagedCommitTs := latestStagedCommitTsByIdentifier(stagedFiles)
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

	prepared := make([]preparedStagedFile, 0, len(eligible))
	candidateBatchIDsByIdentifier := make(map[string][]string)
	candidateBatchIDSeenByIdentifier := make(map[string]map[string]struct{})
	candidateRowIDsByIdentifier := make(map[string][]string)
	candidateRowIDSeenByIdentifier := make(map[string]map[string]struct{})
	identifierByKey := make(map[string][]string)
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
		prepared = append(prepared, preparedStagedFile{file: staged, rowIDs: rowIDs})
		if _, ok := identifierByKey[key]; !ok {
			identifierByKey[key] = append([]string(nil), staged.batch.Identifier...)
		}
		seenBatchIDs := candidateBatchIDSeenByIdentifier[key]
		if seenBatchIDs == nil {
			seenBatchIDs = make(map[string]struct{})
			candidateBatchIDSeenByIdentifier[key] = seenBatchIDs
		}
		if _, ok := seenBatchIDs[staged.batch.BatchID]; !ok {
			candidateBatchIDsByIdentifier[key] = append(candidateBatchIDsByIdentifier[key], staged.batch.BatchID)
			seenBatchIDs[staged.batch.BatchID] = struct{}{}
		}
		seenRowIDs := candidateRowIDSeenByIdentifier[key]
		if seenRowIDs == nil {
			seenRowIDs = make(map[string]struct{})
			candidateRowIDSeenByIdentifier[key] = seenRowIDs
		}
		for _, rowID := range rowIDs {
			if _, ok := seenRowIDs[rowID]; ok {
				continue
			}
			candidateRowIDsByIdentifier[key] = append(candidateRowIDsByIdentifier[key], rowID)
			seenRowIDs[rowID] = struct{}{}
		}
	}

	committedByIdentifier := make(map[string]map[string]struct{}, len(candidateBatchIDsByIdentifier))
	for key, batchIDs := range candidateBatchIDsByIdentifier {
		committedBatches, err := s.committedBatches(ctx, writer, identifierByKey[key], batchIDs)
		if err != nil {
			s.recordAppendFailure("committed_batch_lookup")
			log.Warn("iceberg committer will retry after committed batch lookup failure",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", identifierByKey[key]),
				zap.Error(err))
			s.resetWriter()
			return nil
		}
		committedByIdentifier[key] = committedBatches
	}
	committedRowIDsByIdentifier := make(map[string]map[string]struct{}, len(candidateRowIDsByIdentifier))
	for key, rowIDs := range candidateRowIDsByIdentifier {
		committedRowIDs, err := s.durableCommittedRowIDs(ctx, identifierByKey[key], rowIDs)
		if err != nil {
			s.recordAppendFailure("committed_row_lookup")
			log.Warn("iceberg committer will retry after committed row lookup failure",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", identifierByKey[key]),
				zap.Error(err))
			s.resetWriter()
			return nil
		}
		committedRowIDsByIdentifier[key] = committedRowIDs
	}

	currentGroups := make(map[string]*stagedDrainGroup)
	rowIDsByIdentifier := make(map[string]map[string]struct{})
	groups := make([]*stagedDrainGroup, 0)
	deleteQueue := make([]stagedFile, 0, len(eligible))
	for _, preparedFile := range prepared {
		staged := preparedFile.file
		rowIDs := preparedFile.rowIDs
		key := identifierKey(staged.batch.Identifier)
		seenRowIDs := rowIDsByIdentifier[key]
		if seenRowIDs == nil {
			seenRowIDs = s.cachedCommittedRowIDs(key)
			if seenRowIDs == nil {
				seenRowIDs = make(map[string]struct{}, len(staged.batch.Rows))
			}
			rememberRowIDSet(seenRowIDs, committedRowIDsByIdentifier[key])
			rowIDsByIdentifier[key] = seenRowIDs
		}
		if _, committed := committedByIdentifier[key][staged.batch.BatchID]; committed {
			rememberRowIDs(seenRowIDs, rowIDs)
			s.rememberCommittedRowIDs(key, rowIDs)
			if err := s.markBatchesCommitted(ctx, staged.batch.Identifier, []string{staged.batch.BatchID}, staged.batch.MaxCommitTs, len(staged.batch.Rows)); err != nil {
				return errors.Trace(err)
			}
			if err := s.markRowsCommitted(ctx, staged.batch.Identifier, rowIDs, []string{staged.batch.BatchID}, staged.batch.MaxCommitTs); err != nil {
				return errors.Trace(err)
			}
			deleteQueued := stagedBatchSafeToDelete(staged.batch, checkpointTs, latestStagedCommitTs[key])
			if deleteQueued {
				deleteQueue = append(deleteQueue, staged)
			}
			log.Info("iceberg committer skipped already committed staged batch",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", staged.batch.Identifier),
				zap.String("batchID", staged.batch.BatchID),
				zap.Int("rows", len(staged.batch.Rows)),
				zap.Uint64("maxCommitTs", staged.batch.MaxCommitTs),
				zap.Bool("deleteQueued", deleteQueued))
			continue
		}

		uniqueRows, uniqueRowIDs, err := uniqueStagedRows(staged.batch.Rows, rowIDs, seenRowIDs)
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
		group.rowIDs = append(group.rowIDs, uniqueRowIDs...)
		group.batchIDs = append(group.batchIDs, staged.batch.BatchID)
		if staged.batch.MaxCommitTs > group.maxCommitTs {
			group.maxCommitTs = staged.batch.MaxCommitTs
		}
	}

	for _, group := range groups {
		if len(group.rows) == 0 {
			if err := s.markBatchesCommitted(ctx, group.identifier, group.batchIDs, group.maxCommitTs, 0); err != nil {
				return errors.Trace(err)
			}
			groupKey := identifierKey(group.identifier)
			deleteQueued := stagedGroupSafeToDelete(group, checkpointTs, latestStagedCommitTs[groupKey])
			if deleteQueued {
				deleteQueue = append(deleteQueue, group.files...)
			}
			log.Info("iceberg committer handled duplicate staged rows",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Strings("identifier", group.identifier),
				zap.Int("files", len(group.files)),
				zap.Strings("batchIDs", group.batchIDs),
				zap.Uint64("maxCommitTs", group.maxCommitTs),
				zap.Uint64("checkpointTs", checkpointTs),
				zap.Bool("deleteQueued", deleteQueued))
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
		var err error
		writer, err = s.appendRowsWithCommitConflictRetry(ctx, writer, group, props)
		if err != nil {
			s.recordCommitDuration("error", time.Since(appendStart))
			s.recordAppendFailure(appendFailureReason(err))
			if errors.Is(err, errIcebergTargetOwnerConflict) || errors.Cause(err) == errIcebergTargetOwnerConflict {
				s.recordTargetOwnerConflict(err)
				return errors.Trace(err)
			}
			committed, checkErr := allBatchesCommitted(ctx, writer, group.identifier, group.batchIDs)
			if checkErr == nil && committed {
				if err := s.markBatchesCommitted(ctx, group.identifier, group.batchIDs, group.maxCommitTs, len(group.rows)); err != nil {
					return errors.Trace(err)
				}
				if err := s.markRowsCommitted(ctx, group.identifier, group.rowIDs, group.batchIDs, group.maxCommitTs); err != nil {
					return errors.Trace(err)
				}
				s.rememberCommittedRowIDs(identifierKey(group.identifier), group.rowIDs)
				groupKey := identifierKey(group.identifier)
				if stagedGroupSafeToDelete(group, checkpointTs, latestStagedCommitTs[groupKey]) {
					deleteQueue = append(deleteQueue, group.files...)
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
		if err := s.markBatchesCommitted(ctx, group.identifier, group.batchIDs, group.maxCommitTs, len(group.rows)); err != nil {
			return errors.Trace(err)
		}
		if err := s.markRowsCommitted(ctx, group.identifier, group.rowIDs, group.batchIDs, group.maxCommitTs); err != nil {
			return errors.Trace(err)
		}
		s.rememberCommittedRowIDs(identifierKey(group.identifier), group.rowIDs)
		s.recordCommitDuration("success", time.Since(appendStart))
		s.recordCommitSuccess(len(group.batchIDs), len(group.rows), checkpointTs, group.maxCommitTs)
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
		groupKey := identifierKey(group.identifier)
		deleteQueued := stagedGroupSafeToDelete(group, checkpointTs, latestStagedCommitTs[groupKey])
		if deleteQueued {
			deleteQueue = append(deleteQueue, group.files...)
		}
		log.Info("iceberg committer appended staged rows",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Strings("identifier", group.identifier),
			zap.Int("rows", len(group.rows)),
			zap.Int("batches", len(group.batchIDs)),
			zap.Uint64("maxCommitTs", group.maxCommitTs),
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Bool("deleteQueued", deleteQueued))
	}
	if len(deleteQueue) > 0 {
		if err := s.deleteStagedFiles(deleteQueue); err != nil {
			return errors.Trace(err)
		}
	}
	if err := s.refreshStageMetrics(ctx, checkpointTs); err != nil {
		log.Warn("failed to refresh iceberg staged metrics",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Error(err))
	}
	return nil
}

func (s *sink) appendRowsWithCommitConflictRetry(
	ctx context.Context,
	writer appendWriter,
	group *stagedDrainGroup,
	props iceberggo.Properties,
) (appendWriter, error) {
	var err error
	for attempt := 0; attempt <= maxCommitConflictRetry; attempt++ {
		err = writer.AppendRows(ctx, group.identifier, group.rows, group.tableSchema, props)
		if err == nil {
			return writer, nil
		}
		if !isIcebergCommitFailed(err) || attempt == maxCommitConflictRetry {
			return writer, err
		}
		s.recordAppendFailure("commit_conflict_retry")
		log.Warn("iceberg committer retrying append after optimistic commit conflict",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Strings("identifier", group.identifier),
			zap.Int("rows", len(group.rows)),
			zap.Strings("batchIDs", group.batchIDs),
			zap.Int("attempt", attempt+1),
			zap.Int("maxAttempts", maxCommitConflictRetry+1),
			zap.Error(err))
		s.resetWriter()
		nextWriter, getErr := s.getWriter(ctx)
		if getErr != nil {
			return writer, errors.Trace(getErr)
		}
		writer = nextWriter
	}
	return writer, err
}

func (s *sink) committedBatches(
	ctx context.Context,
	writer appendWriter,
	identifier []string,
	batchIDs []string,
) (map[string]struct{}, error) {
	committedBatches, err := writer.CommittedBatches(ctx, identifier, batchIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if committedBatches == nil {
		committedBatches = make(map[string]struct{})
	}
	ledgerBatches, err := s.stage.CommittedBatchesForCandidates(ctx, s.changefeedID.String(), identifier, batchIDs)
	if err != nil {
		s.recordCommittedLedgerLookup("error")
		return nil, errors.Trace(err)
	}
	s.recordCommittedLedgerLookup("success")
	for batchID := range ledgerBatches {
		committedBatches[batchID] = struct{}{}
	}
	return committedBatches, nil
}

func (s *sink) durableCommittedRowIDs(
	ctx context.Context,
	identifier []string,
	rowIDs []string,
) (map[string]struct{}, error) {
	committedRowIDs, err := s.stage.CommittedRowIDsForCandidates(ctx, s.changefeedID.String(), identifier, rowIDs)
	if err != nil {
		s.recordCommittedRowLedgerLookup("error")
		return nil, errors.Trace(err)
	}
	s.recordCommittedRowLedgerLookup("success")
	return committedRowIDs, nil
}

func (s *sink) markBatchesCommitted(
	ctx context.Context,
	identifier []string,
	batchIDs []string,
	maxCommitTs uint64,
	rowCount int,
) error {
	created, err := s.stage.MarkBatchesCommitted(ctx, s.changefeedID.String(), identifier, batchIDs, maxCommitTs, rowCount)
	if err != nil {
		s.recordCommittedLedgerWrite("error")
		return errors.Trace(err)
	}
	s.recordCommittedLedgerWrite("success")
	if created > 0 {
		metrics.IcebergCommittedLedgerEntriesGauge.WithLabelValues(
			s.changefeedID.Keyspace(), s.changefeedID.Name()).Add(float64(created))
	}
	return nil
}

func (s *sink) markRowsCommitted(
	ctx context.Context,
	identifier []string,
	rowIDs []string,
	batchIDs []string,
	maxCommitTs uint64,
) error {
	created, err := s.stage.MarkRowsCommitted(ctx, s.changefeedID.String(), identifier, rowIDs, batchIDs, maxCommitTs)
	if err != nil {
		s.recordCommittedRowLedgerWrite("error")
		return errors.Trace(err)
	}
	s.recordCommittedRowLedgerWrite("success")
	if created > 0 {
		metrics.IcebergCommittedRowLedgerEntriesGauge.WithLabelValues(
			s.changefeedID.Keyspace(), s.changefeedID.Name()).Add(float64(created))
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
	_, alreadyClaimed := s.ownerClaims[key]
	s.ownerClaimsMu.Unlock()
	if err := s.claimTargetCommitterLease(ctx, identifier); err != nil {
		s.recordTargetOwnerConflict(err)
		return errors.Trace(err)
	}
	if err := s.stage.ClaimTargetOwner(ctx, s.changefeedID.String(), identifier); err != nil {
		s.recordTargetOwnerConflict(err)
		s.releaseTargetCommitterLease(key)
		return errors.Trace(err)
	}
	claim := icebergcfg.NewTargetOwnerClaim(
		s.cfg.TiCDCClusterID,
		s.cfg.UpstreamID,
		s.changefeedID.String(),
		identifier)
	if err := icebergcfg.ClaimTargetOwner(ctx, s.cfg.Warehouse, claim); err != nil {
		s.recordTargetOwnerConflict(err)
		s.releaseTargetCommitterLease(key)
		return errors.Trace(err)
	}
	if !alreadyClaimed {
		if err := s.reconcileTargetOnTakeover(ctx, identifier); err != nil {
			s.releaseTargetCommitterLease(key)
			return errors.Trace(err)
		}
	}
	s.ownerClaimsMu.Lock()
	s.ownerClaims[key] = struct{}{}
	s.ownerClaimsMu.Unlock()
	return nil
}

func (s *sink) reconcileTargetOnTakeover(ctx context.Context, identifier []string) error {
	writer, err := s.getWriter(ctx)
	if err != nil {
		s.recordAppendFailure("writer_init")
		s.resetWriter()
		return errors.Trace(err)
	}
	reconciler, ok := writer.(targetTakeoverReconciler)
	if !ok {
		return nil
	}
	return reconciler.ReconcileTargetOnTakeover(ctx, identifier, s.ownerID, s.changefeedID.String())
}

func (s *sink) claimTargetCommitterLease(ctx context.Context, identifier []string) error {
	session, ok := appcontext.GetServiceIfExists[*concurrency.Session](appcontext.EtcdSession)
	if !ok || session == nil {
		return nil
	}
	select {
	case <-session.Done():
		return errors.ErrEtcdSessionDone.GenWithStackByArgs()
	default:
	}

	key := identifierKey(identifier)
	s.ownerClaimsMu.Lock()
	if election := s.ownerLeases[key]; election != nil {
		s.ownerClaimsMu.Unlock()
		return nil
	}
	s.ownerClaimsMu.Unlock()

	claim := icebergcfg.NewTargetOwnerClaim(
		s.cfg.TiCDCClusterID,
		s.cfg.UpstreamID,
		s.changefeedID.String(),
		identifier)
	payload, err := json.Marshal(&claim)
	if err != nil {
		return errors.Trace(err)
	}

	electionPrefix := etcd.IcebergCommitterElectionKey(s.cfg.TiCDCClusterID, key)
	election := concurrency.NewElection(session, electionPrefix)
	if err := s.validateExistingTargetLeader(ctx, election, string(payload)); err != nil {
		return errors.Trace(err)
	}

	timeout := time.Duration(config.GetGlobalServerConfig().CaptureSessionTTL+1) * time.Second
	if timeout < 5*time.Second {
		timeout = 5 * time.Second
	}
	campaignCtx, cancel := context.WithTimeout(ctx, timeout)
	err = election.Campaign(campaignCtx, string(payload))
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return errors.Trace(ctx.Err())
		}
		if leaderErr := s.validateExistingTargetLeader(context.Background(), election, string(payload)); leaderErr != nil {
			return errors.Trace(leaderErr)
		}
		return errors.Trace(err)
	}

	s.ownerClaimsMu.Lock()
	defer s.ownerClaimsMu.Unlock()
	if existing := s.ownerLeases[key]; existing != nil {
		_ = election.Resign(context.Background())
		return nil
	}
	s.ownerLeases[key] = election
	log.Info("iceberg target committer lease acquired",
		zap.String("changefeed", s.changefeedID.String()),
		zap.Strings("identifier", identifier),
		zap.String("electionPrefix", electionPrefix))
	return nil
}

func (s *sink) validateExistingTargetLeader(ctx context.Context, election *concurrency.Election, expected string) error {
	resp, err := election.Leader(ctx)
	if err != nil {
		if errors.Is(err, concurrency.ErrElectionNoLeader) {
			return nil
		}
		return errors.Trace(err)
	}
	for _, kv := range resp.Kvs {
		if string(kv.Value) == expected {
			return nil
		}
		var existing icebergcfg.TargetOwnerClaim
		var expectedClaim icebergcfg.TargetOwnerClaim
		_ = json.Unmarshal(kv.Value, &existing)
		_ = json.Unmarshal([]byte(expected), &expectedClaim)
		return errors.Annotatef(errIcebergTargetOwnerConflict,
			"etcd committer lease owner %q conflicts with owner %q", existing.OwnerID, expectedClaim.OwnerID)
	}
	return nil
}

func (s *sink) releaseTargetCommitterLease(key string) {
	s.ownerClaimsMu.Lock()
	election := s.ownerLeases[key]
	delete(s.ownerLeases, key)
	s.ownerClaimsMu.Unlock()
	if election != nil {
		_ = election.Resign(context.Background())
	}
}

func (s *sink) releaseTargetCommitterLeases() {
	s.ownerClaimsMu.Lock()
	leases := make([]*concurrency.Election, 0, len(s.ownerLeases))
	for key, election := range s.ownerLeases {
		leases = append(leases, election)
		delete(s.ownerLeases, key)
	}
	s.ownerClaimsMu.Unlock()
	for _, election := range leases {
		_ = election.Resign(context.Background())
	}
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
	metrics.IcebergStagedBytesGauge.WithLabelValues(keyspace, changefeed, "pending").Set(float64(stagedBytes(stagedFiles)))
	metrics.IcebergStagedBytesGauge.WithLabelValues(keyspace, changefeed, "eligible").Set(float64(stagedBytes(eligible)))
	metrics.IcebergStagingBackendInfoGauge.WithLabelValues(keyspace, changefeed, "local_json").Set(1)
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

func (s *sink) recordCommitSuccess(batchCount int, rowCount int, checkpointTs uint64, maxCommitTs uint64) {
	keyspace, changefeed := s.changefeedID.Keyspace(), s.changefeedID.Name()
	metrics.IcebergCommittedBatchesCounter.WithLabelValues(keyspace, changefeed).Add(float64(batchCount))
	metrics.IcebergCommittedRowsCounter.WithLabelValues(keyspace, changefeed).Add(float64(rowCount))
	if checkpointTs >= maxCommitTs {
		metrics.IcebergCommitBarrierLagGauge.WithLabelValues(keyspace, changefeed).Set(float64(checkpointTs - maxCommitTs))
		return
	}
	metrics.IcebergNegativeCommitBarrierLagCounter.WithLabelValues(keyspace, changefeed).Inc()
	metrics.IcebergCommitBarrierLagGauge.WithLabelValues(keyspace, changefeed).Set(0)
}

func (s *sink) recordAppendFailure(reason string) {
	metrics.IcebergAppendFailureCounter.WithLabelValues(
		s.changefeedID.Keyspace(), s.changefeedID.Name(), reason).Inc()
}

func (s *sink) recordCommittedLedgerWrite(result string) {
	metrics.IcebergCommittedLedgerWritesCounter.WithLabelValues(
		s.changefeedID.Keyspace(), s.changefeedID.Name(), result).Inc()
}

func (s *sink) recordCommittedLedgerLookup(result string) {
	metrics.IcebergCommittedLedgerLookupsCounter.WithLabelValues(
		s.changefeedID.Keyspace(), s.changefeedID.Name(), result).Inc()
}

func (s *sink) recordCommittedRowLedgerWrite(result string) {
	metrics.IcebergCommittedRowLedgerWritesCounter.WithLabelValues(
		s.changefeedID.Keyspace(), s.changefeedID.Name(), result).Inc()
}

func (s *sink) recordCommittedRowLedgerLookup(result string) {
	metrics.IcebergCommittedRowLedgerLookupsCounter.WithLabelValues(
		s.changefeedID.Keyspace(), s.changefeedID.Name(), result).Inc()
}

func appendFailureReason(err error) string {
	if errors.Is(err, errIcebergTargetOwnerConflict) || errors.Cause(err) == errIcebergTargetOwnerConflict {
		return "owner_conflict"
	}
	if isIcebergCommitFailed(err) {
		return "commit_conflict"
	}
	return "append"
}

func isIcebergCommitFailed(err error) bool {
	return errors.Is(err, icebergrest.ErrCommitFailed) || errors.Cause(err) == icebergrest.ErrCommitFailed
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
	metrics.IcebergStagedBytesGauge.DeleteLabelValues(keyspace, changefeed, "pending")
	metrics.IcebergStagedBytesGauge.DeleteLabelValues(keyspace, changefeed, "eligible")
	metrics.IcebergCommitBarrierLagGauge.DeleteLabelValues(keyspace, changefeed)
	metrics.IcebergCommittedLedgerEntriesGauge.DeleteLabelValues(keyspace, changefeed)
	metrics.IcebergCommittedRowLedgerEntriesGauge.DeleteLabelValues(keyspace, changefeed)
	metrics.IcebergStagingBackendInfoGauge.DeleteLabelValues(keyspace, changefeed, "local_json")
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

func stagedBytes(stagedFiles []stagedFile) int64 {
	var bytes int64
	for _, staged := range stagedFiles {
		bytes += staged.sizeBytes
	}
	return bytes
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

func stagedGroupSafeToDelete(group *stagedDrainGroup, checkpointTs uint64, latestStagedCommitTs uint64) bool {
	return checkpointTs != 0 && group.maxCommitTs <= checkpointTs && latestStagedCommitTs <= checkpointTs
}

func stagedBatchSafeToDelete(batch stagedBatch, checkpointTs uint64, latestStagedCommitTs uint64) bool {
	return checkpointTs != 0 && batch.MaxCommitTs <= checkpointTs && latestStagedCommitTs <= checkpointTs
}

func latestStagedCommitTsByIdentifier(stagedFiles []stagedFile) map[string]uint64 {
	latest := make(map[string]uint64)
	for _, staged := range stagedFiles {
		key := identifierKey(staged.batch.Identifier)
		if staged.batch.MaxCommitTs > latest[key] {
			latest[key] = staged.batch.MaxCommitTs
		}
	}
	return latest
}

func uniqueStagedRows(
	rows []map[string]any,
	rowIDs []string,
	seenRowIDs map[string]struct{},
) ([]map[string]any, []string, error) {
	if len(rowIDs) != len(rows) {
		return nil, nil, errors.Errorf("iceberg staged row id count %d does not match row count %d",
			len(rowIDs), len(rows))
	}
	uniqueRows := make([]map[string]any, 0, len(rows))
	uniqueRowIDs := make([]string, 0, len(rows))
	for i, rowID := range rowIDs {
		if _, ok := seenRowIDs[rowID]; ok {
			continue
		}
		seenRowIDs[rowID] = struct{}{}
		uniqueRows = append(uniqueRows, rows[i])
		uniqueRowIDs = append(uniqueRowIDs, rowID)
	}
	return uniqueRows, uniqueRowIDs, nil
}

func rememberRowIDs(seenRowIDs map[string]struct{}, rowIDs []string) {
	for _, rowID := range rowIDs {
		seenRowIDs[rowID] = struct{}{}
	}
}

func rememberRowIDSet(seenRowIDs map[string]struct{}, rowIDs map[string]struct{}) {
	for rowID := range rowIDs {
		seenRowIDs[rowID] = struct{}{}
	}
}

func (s *sink) cachedCommittedRowIDs(key string) map[string]struct{} {
	cache := s.committedRowIDsByIdentifier[key]
	if cache == nil || len(cache.set) == 0 {
		return nil
	}
	copied := make(map[string]struct{}, len(cache.set))
	for rowID := range cache.set {
		copied[rowID] = struct{}{}
	}
	return copied
}

func (s *sink) rememberCommittedRowIDs(key string, rowIDs []string) {
	if len(rowIDs) == 0 {
		return
	}
	cache := s.committedRowIDsByIdentifier[key]
	if cache == nil {
		cache = &committedRowIDCache{set: make(map[string]struct{}, len(rowIDs))}
		s.committedRowIDsByIdentifier[key] = cache
	}
	for _, rowID := range rowIDs {
		if rowID == "" {
			continue
		}
		if _, ok := cache.set[rowID]; ok {
			continue
		}
		cache.set[rowID] = struct{}{}
		cache.order = append(cache.order, rowID)
	}
	if extra := len(cache.order) - maxCommittedRowIDCache; extra > 0 {
		for _, rowID := range cache.order[:extra] {
			delete(cache.set, rowID)
		}
		cache.order = append([]string(nil), cache.order[extra:]...)
	}
}

func allBatchesCommitted(ctx context.Context, writer appendWriter, identifier []string, batchIDs []string) (bool, error) {
	committedBatches, err := writer.CommittedBatches(ctx, identifier, batchIDs)
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

func (s *sink) stageAllAndDrain(ctx context.Context, buffers map[string]*tableBuffer, checkpointTs uint64) error {
	if checkpointTs == 0 && s.checkpointPending.Load() {
		checkpointTs = s.latestCheckpointTs.Load()
	}
	if err := s.stageAll(ctx, buffers); err != nil {
		return errors.Trace(err)
	}
	if s.isCommitter.Load() {
		failpoint.Inject("IcebergSinkBlockBeforeDrain", nil)
		if err := s.drainStaged(ctx, checkpointTs); err != nil {
			return errors.Trace(err)
		}
	}
	if checkpointTs != 0 && checkpointTs >= s.latestCheckpointTs.Load() {
		s.checkpointPending.Store(false)
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
