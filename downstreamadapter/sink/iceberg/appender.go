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
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceberggo "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

type icebergAppender struct {
	catalog         catalog.Catalog
	tableProperties map[string]string

	mu     sync.Mutex
	tables map[string]*icebergtable.Table
}

type namespaceEnsurer interface {
	CreateNamespace(context.Context, icebergtable.Identifier, iceberggo.Properties) error
	CheckNamespaceExists(context.Context, icebergtable.Identifier) (bool, error)
}

func newIcebergAppender(ctx context.Context, cfg *icebergcfg.Config) (*icebergAppender, error) {
	ctx, opts, err := restCatalogOptions(ctx, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cat, err := rest.NewCatalog(ctx, "ticdc", cfg.CatalogURI, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &icebergAppender{
		catalog:         cat,
		tableProperties: cloneStringMap(cfg.TableProperties),
		tables:          make(map[string]*icebergtable.Table),
	}, nil
}

func restCatalogOptions(
	ctx context.Context,
	cfg *icebergcfg.Config,
) (context.Context, []rest.Option, error) {
	opts := []rest.Option{rest.WithWarehouseLocation(cfg.Warehouse)}
	if len(cfg.SuppressHeaders) > 0 {
		headers := make(map[string]string, len(cfg.SuppressHeaders))
		for _, header := range cfg.SuppressHeaders {
			headers[http.CanonicalHeaderKey(header)] = ""
		}
		opts = append(opts, rest.WithHeaders(headers))
	}
	if cfg.CatalogHostHeader != "" || len(cfg.SuppressHeaders) > 0 {
		opts = append(opts, rest.WithCustomTransport(&catalogHeaderTransport{
			base:            http.DefaultTransport,
			host:            cfg.CatalogHostHeader,
			suppressHeaders: cfg.SuppressHeaders,
		}))
	}
	if cfg.AWSRegion != "" || cfg.AWSUserAgent != "" {
		loadOpts := make([]func(*awsconfig.LoadOptions) error, 0, 2)
		if cfg.AWSRegion != "" {
			loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.AWSRegion))
		}
		if cfg.AWSUserAgent != "" {
			loadOpts = append(loadOpts, awsconfig.WithAPIOptions([]func(*smithymiddleware.Stack) error{
				awsmiddleware.AddUserAgentKey(cfg.AWSUserAgent),
			}))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		opts = append(opts, rest.WithAwsConfig(awsCfg))
	}
	return ctx, opts, nil
}

type catalogHeaderTransport struct {
	base            http.RoundTripper
	host            string
	suppressHeaders []string
}

func (t *catalogHeaderTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	if t.host != "" {
		clone.Host = t.host
	}
	for _, header := range t.suppressHeaders {
		header = strings.TrimSpace(header)
		if header != "" {
			clone.Header.Del(header)
		}
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(clone)
}

func (a *icebergAppender) AppendRows(
	ctx context.Context,
	identifier []string,
	rows []map[string]any,
	tableSchema *stagedTableSchema,
	snapshotProps iceberggo.Properties,
) error {
	if len(rows) == 0 {
		return nil
	}

	ownerID := snapshotProps[snapshotOwnerIDKey]
	changefeed := snapshotProps[snapshotChangefeedKey]
	tbl, err := a.loadOrCreateTable(ctx, identifier, rows, tableSchema, ownerID, changefeed,
		snapshotProps[snapshotCDCClusterIDKey], snapshotProps[snapshotUpstreamIDKey])
	if err != nil {
		return errors.Trace(err)
	}

	arrowSchema, err := icebergtable.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		return errors.Trace(err)
	}

	record, err := rowsToRecordBatch(rows, arrowSchema)
	if err != nil {
		return errors.Trace(err)
	}
	defer record.Release()

	reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
	if err != nil {
		return errors.Trace(err)
	}
	defer reader.Release()

	updated, err := tbl.Append(ctx, reader, snapshotProps)
	if err != nil {
		return errors.Trace(err)
	}

	a.mu.Lock()
	a.tables[identifierKey(identifier)] = updated
	a.mu.Unlock()
	return nil
}

func (a *icebergAppender) CommittedBatches(
	ctx context.Context,
	identifier []string,
	batchIDs []string,
) (map[string]struct{}, error) {
	tbl, err := a.catalog.LoadTable(ctx, icebergtable.Identifier(identifier))
	if err != nil {
		if stderrors.Is(err, catalog.ErrNoSuchTable) {
			return nil, nil
		}
		return nil, fmt.Errorf("load iceberg table %q: %w", identifier, err)
	}

	a.mu.Lock()
	a.tables[identifierKey(identifier)] = tbl
	a.mu.Unlock()
	return tableCommittedBatchesForCandidates(tbl, batchIDs), nil
}

func (a *icebergAppender) ReconcileTargetOnTakeover(
	ctx context.Context,
	identifier []string,
	ownerID string,
	changefeed string,
) error {
	tbl, err := a.catalog.LoadTable(ctx, icebergtable.Identifier(identifier))
	if err != nil {
		if stderrors.Is(err, catalog.ErrNoSuchTable) {
			return nil
		}
		return fmt.Errorf("load iceberg table %q for takeover reconciliation: %w", identifier, err)
	}
	if err := validateTargetOwner(tbl, ownerID, changefeed); err != nil {
		return errors.Trace(err)
	}
	result, err := tbl.DeleteOrphanFiles(ctx,
		icebergtable.WithDryRun(true),
		icebergtable.WithFilesOlderThan(0),
		icebergtable.WithLocation(tbl.Location()))
	if err != nil {
		return fmt.Errorf("reconcile iceberg table %q by warehouse listing: %w", identifier, err)
	}
	if len(result.OrphanFileLocations) > 0 {
		log.Warn("iceberg takeover reconciliation found orphan warehouse files",
			zap.Strings("identifier", identifier),
			zap.String("location", tbl.Location()),
			zap.Int("orphanFiles", len(result.OrphanFileLocations)),
			zap.Int64("totalScannedBytes", result.TotalSizeBytes))
	}
	return nil
}

func (a *icebergAppender) ApplyTableSchema(
	ctx context.Context,
	identifier []string,
	tableInfo *common.TableInfo,
	ddlType timodel.ActionType,
	ownerID string,
	changefeed string,
	cdcClusterID string,
	upstreamID string,
) error {
	if tableInfo == nil {
		return errors.New("iceberg schema update has no table info")
	}
	tableSchema := stagedTableSchemaForTableInfo(tableInfo)
	if tableSchema == nil {
		return errors.New("iceberg schema update has no staged table schema")
	}

	ident := icebergtable.Identifier(identifier)
	tbl, err := a.catalog.LoadTable(ctx, ident)
	if err != nil {
		if !stderrors.Is(err, catalog.ErrNoSuchTable) {
			return fmt.Errorf("load iceberg table %q for schema DDL: %w", identifier, err)
		}
		tbl, err = a.createTable(ctx, identifier, nil, tableSchema, ownerID, changefeed, cdcClusterID, upstreamID)
		if err != nil {
			return errors.Trace(err)
		}
	} else if err := validateTargetOwner(tbl, ownerID, changefeed); err != nil {
		return errors.Trace(err)
	}

	updated, err := evolveCDCLogTableSchema(ctx, tbl, tableSchema, ddlType)
	if err != nil {
		return errors.Trace(err)
	}
	a.mu.Lock()
	a.tables[identifierKey(identifier)] = updated
	a.mu.Unlock()
	return nil
}

func evolveCDCLogTableSchema(
	ctx context.Context,
	tbl *icebergtable.Table,
	tableSchema *stagedTableSchema,
	ddlType timodel.ActionType,
) (*icebergtable.Table, error) {
	txn := tbl.NewTransaction()
	updateSchema := txn.UpdateSchema(true, true)
	desiredByName := stagedColumnsByName(tableSchema)
	changed := false

	for _, parent := range []string{"data", "old"} {
		existingByName, err := icebergStructFieldsByName(tbl.Schema(), parent)
		if err != nil {
			return nil, err
		}
		added, removed := schemaNameDelta(existingByName, desiredByName)
		if ddlType == timodel.ActionModifyColumn && len(added) == 1 && len(removed) == 1 {
			oldName, newName := firstMapKey(removed), firstMapKey(added)
			updateSchema.RenameColumn([]string{parent, oldName}, newName)
			existingByName[newName] = existingByName[oldName]
			delete(existingByName, oldName)
			delete(added, newName)
			delete(removed, oldName)
			changed = true
		}
		// DROP COLUMN is absorbed for CDC-log tables: old fields stay nullable
		// so pre-DDL rows and readers keep a stable schema.
		for name, desired := range desiredByName {
			desiredType := icebergTypeForStagedColumn(desired)
			existing, ok := existingByName[name]
			if !ok {
				updateSchema.AddColumn([]string{parent, name}, desiredType, "", false, nil)
				changed = true
				continue
			}
			if !existing.Type.Equals(desiredType) {
				if !canPromoteIcebergType(existing.Type, desiredType) {
					return nil, errors.Errorf("iceberg schema DDL cannot narrow column %s.%s from %s to %s",
						parent, name, existing.Type, desiredType)
				}
				updateSchema.UpdateColumn([]string{parent, name}, icebergtable.ColumnUpdate{
					FieldType: iceberggo.Optional[iceberggo.Type]{Valid: true, Val: desiredType},
				})
				changed = true
			}
		}
	}
	if !changed {
		return tbl, nil
	}
	if err := updateSchema.Commit(); err != nil {
		return nil, err
	}
	return txn.Commit(ctx)
}

func stagedColumnsByName(tableSchema *stagedTableSchema) map[string]stagedColumnSchema {
	columns := make(map[string]stagedColumnSchema, len(tableSchema.Columns))
	for _, column := range tableSchema.Columns {
		columns[column.Name] = column
	}
	return columns
}

func icebergStructFieldsByName(schema *iceberggo.Schema, parent string) (map[string]iceberggo.NestedField, error) {
	field, ok := schema.FindFieldByName(parent)
	if !ok {
		return nil, fmt.Errorf("iceberg CDC schema is missing %q struct", parent)
	}
	structType, ok := field.Type.(*iceberggo.StructType)
	if !ok {
		return nil, fmt.Errorf("iceberg CDC schema field %q is %T, expected struct", parent, field.Type)
	}
	fields := make(map[string]iceberggo.NestedField, len(structType.Fields()))
	for _, nested := range structType.Fields() {
		fields[nested.Name] = nested
	}
	return fields, nil
}

func canPromoteIcebergType(existing iceberggo.Type, desired iceberggo.Type) bool {
	if existing.Equals(desired) {
		return true
	}
	switch oldType := existing.(type) {
	case iceberggo.Int32Type:
		return desired.Equals(iceberggo.PrimitiveTypes.Int64) ||
			desired.Equals(iceberggo.PrimitiveTypes.Float64)
	case iceberggo.Int64Type:
		return desired.Equals(iceberggo.PrimitiveTypes.Float64)
	case iceberggo.Float32Type:
		return desired.Equals(iceberggo.PrimitiveTypes.Float64)
	case iceberggo.DecimalType:
		newType, ok := desired.(iceberggo.DecimalType)
		if !ok {
			return false
		}
		oldIntegerDigits := oldType.Precision() - oldType.Scale()
		newIntegerDigits := newType.Precision() - newType.Scale()
		return newIntegerDigits >= oldIntegerDigits && newType.Scale() >= oldType.Scale()
	default:
		return false
	}
}

func schemaNameDelta(
	existing map[string]iceberggo.NestedField,
	desired map[string]stagedColumnSchema,
) (map[string]struct{}, map[string]struct{}) {
	added := make(map[string]struct{})
	for name := range desired {
		if _, ok := existing[name]; !ok {
			added[name] = struct{}{}
		}
	}
	removed := make(map[string]struct{})
	for name := range existing {
		if _, ok := desired[name]; !ok {
			removed[name] = struct{}{}
		}
	}
	return added, removed
}

func firstMapKey[T any](values map[string]T) string {
	for value := range values {
		return value
	}
	return ""
}

func tableCommittedBatchesForCandidates(tbl *icebergtable.Table, batchIDs []string) map[string]struct{} {
	return snapshotCommittedBatchesForCandidates(tbl.Metadata().Snapshots(), batchIDs)
}

func snapshotCommittedBatchesForCandidates(snapshots []icebergtable.Snapshot, batchIDs []string) map[string]struct{} {
	if len(batchIDs) == 0 {
		return nil
	}
	candidates := make(map[string]struct{}, len(batchIDs))
	for _, batchID := range batchIDs {
		if batchID != "" {
			candidates[batchID] = struct{}{}
		}
	}
	committedBatches := make(map[string]struct{}, len(candidates))
	for i := len(snapshots) - 1; i >= 0 && len(committedBatches) < len(candidates); i-- {
		snapshot := snapshots[i]
		if snapshot.Summary == nil {
			continue
		}
		for _, committed := range batchIDsFromProps(snapshot.Summary.Properties) {
			if _, ok := candidates[committed]; ok {
				committedBatches[committed] = struct{}{}
			}
		}
	}
	return committedBatches
}

func (a *icebergAppender) loadOrCreateTable(
	ctx context.Context,
	identifier []string,
	rows []map[string]any,
	tableSchema *stagedTableSchema,
	ownerID string,
	changefeed string,
	cdcClusterID string,
	upstreamID string,
) (*icebergtable.Table, error) {
	key := identifierKey(identifier)

	a.mu.Lock()
	tbl := a.tables[key]
	a.mu.Unlock()
	if tbl != nil {
		if err := validateTargetOwner(tbl, ownerID, changefeed); err != nil {
			return nil, errors.Trace(err)
		}
		return tbl, nil
	}

	loaded, err := a.catalog.LoadTable(ctx, icebergtable.Identifier(identifier))
	if err != nil {
		if !stderrors.Is(err, catalog.ErrNoSuchTable) {
			return nil, fmt.Errorf("load iceberg table %q: %w", identifier, err)
		}
		loaded, err = a.createTable(ctx, identifier, rows, tableSchema, ownerID, changefeed, cdcClusterID, upstreamID)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else if err := validateTargetOwner(loaded, ownerID, changefeed); err != nil {
		return nil, errors.Trace(err)
	}

	a.mu.Lock()
	a.tables[key] = loaded
	a.mu.Unlock()
	return loaded, nil
}

func (a *icebergAppender) createTable(
	ctx context.Context,
	identifier []string,
	rows []map[string]any,
	tableSchema *stagedTableSchema,
	ownerID string,
	changefeed string,
	cdcClusterID string,
	upstreamID string,
) (*icebergtable.Table, error) {
	ident := icebergtable.Identifier(identifier)
	namespace := catalog.NamespaceFromIdent(ident)
	if err := ensureNamespace(ctx, a.catalog, namespace); err != nil {
		return nil, errors.Trace(err)
	}

	arrowSchema := arrowSchemaForRows(rows)
	if tableSchema != nil {
		arrowSchema = arrowSchemaForStagedTableSchema(tableSchema)
	}
	schema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(arrowSchema, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	partitionSpec, err := cdcLogPartitionSpec(schema)
	if err != nil {
		return nil, errors.Trace(err)
	}

	props := createTableProperties(a.tableProperties, ownerID, changefeed, cdcClusterID, upstreamID)

	tbl, err := a.catalog.CreateTable(ctx, ident, schema,
		catalog.WithPartitionSpec(partitionSpec),
		catalog.WithProperties(props))
	if err != nil {
		if stderrors.Is(err, catalog.ErrTableAlreadyExists) {
			loaded, err := a.catalog.LoadTable(ctx, ident)
			if err != nil {
				return nil, err
			}
			if err := validateTargetOwner(loaded, ownerID, changefeed); err != nil {
				return nil, errors.Trace(err)
			}
			return loaded, nil
		}
		return nil, fmt.Errorf("create iceberg table %q: %w", identifier, err)
	}
	return tbl, nil
}

func createTableProperties(
	tableProperties map[string]string,
	ownerID string,
	changefeed string,
	cdcClusterID string,
	upstreamID string,
) iceberggo.Properties {
	props := make(iceberggo.Properties, len(tableProperties)+6)
	for key, value := range tableProperties {
		props[key] = value
	}
	props["format-version"] = "2"
	props["write.format.default"] = "parquet"
	if ownerID != "" {
		props[tableOwnerIDKey] = ownerID
	}
	if changefeed != "" {
		props[tableOwnerKey] = changefeed
	}
	if cdcClusterID != "" {
		props[tableCDCClusterIDKey] = cdcClusterID
	}
	if upstreamID != "" {
		props[tableUpstreamIDKey] = upstreamID
	}
	return props
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func ensureNamespace(ctx context.Context, cat namespaceEnsurer, namespace icebergtable.Identifier) error {
	if len(namespace) == 0 {
		return nil
	}
	err := cat.CreateNamespace(ctx, namespace, iceberggo.Properties{})
	if err == nil || stderrors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		return nil
	}
	exists, checkErr := cat.CheckNamespaceExists(ctx, namespace)
	if checkErr == nil && exists {
		return nil
	}
	if checkErr != nil {
		return fmt.Errorf("create iceberg namespace %q: %w; check namespace existence: %v",
			namespace, err, checkErr)
	}
	return fmt.Errorf("create iceberg namespace %q: %w", namespace, err)
}

func validateTargetOwner(tbl *icebergtable.Table, ownerID string, changefeed string) error {
	if ownerID == "" && changefeed == "" {
		return nil
	}
	if existing := tbl.Properties()[tableOwnerIDKey]; existing != "" && existing != ownerID {
		return errors.Annotatef(errIcebergTargetOwnerConflict,
			"owner %q conflicts with owner %q", existing, ownerID)
	}
	if existing := tbl.Properties()[tableOwnerKey]; existing != "" && changefeed != "" && existing != changefeed {
		return errors.Annotatef(errIcebergTargetOwnerConflict,
			"owner changefeed %q conflicts with changefeed %q", existing, changefeed)
	}
	for _, snapshot := range tbl.Metadata().Snapshots() {
		if snapshot.Summary == nil {
			continue
		}
		existingOwnerID := snapshot.Summary.Properties[snapshotOwnerIDKey]
		if existingOwnerID != "" && existingOwnerID != ownerID {
			return errors.Annotatef(errIcebergTargetOwnerConflict,
				"snapshot owner %q conflicts with owner %q", existingOwnerID, ownerID)
		}
		if existingOwnerID != "" {
			continue
		}
		existingChangefeed := snapshot.Summary.Properties[snapshotChangefeedKey]
		if existingChangefeed != "" && changefeed != "" && existingChangefeed != changefeed {
			return errors.Annotatef(errIcebergTargetOwnerConflict,
				"snapshot owner changefeed %q conflicts with changefeed %q", existingChangefeed, changefeed)
		}
	}
	return nil
}
