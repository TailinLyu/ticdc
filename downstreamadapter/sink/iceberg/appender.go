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
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceberggo "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/pingcap/ticdc/pkg/errors"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
)

type icebergAppender struct {
	catalog catalog.Catalog

	mu     sync.Mutex
	tables map[string]*icebergtable.Table
}

type namespaceEnsurer interface {
	CreateNamespace(context.Context, icebergtable.Identifier, iceberggo.Properties) error
	CheckNamespaceExists(context.Context, icebergtable.Identifier) (bool, error)
}

func newIcebergAppender(ctx context.Context, cfg *icebergcfg.Config) (*icebergAppender, error) {
	cat, err := rest.NewCatalog(ctx, "ticdc", cfg.CatalogURI, rest.WithWarehouseLocation(cfg.Warehouse))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &icebergAppender{
		catalog: cat,
		tables:  make(map[string]*icebergtable.Table),
	}, nil
}

func (a *icebergAppender) AppendRows(
	ctx context.Context,
	identifier []string,
	rows []map[string]any,
	snapshotProps iceberggo.Properties,
) error {
	if len(rows) == 0 {
		return nil
	}

	ownerID := snapshotProps[snapshotOwnerIDKey]
	changefeed := snapshotProps[snapshotChangefeedKey]
	tbl, err := a.loadOrCreateTable(ctx, identifier, rows, ownerID, changefeed,
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
	return tableCommittedBatches(tbl), nil
}

func tableCommittedBatches(tbl *icebergtable.Table) map[string]struct{} {
	committedBatches := make(map[string]struct{})
	for _, snapshot := range tbl.Metadata().Snapshots() {
		if snapshot.Summary == nil {
			continue
		}
		for _, committed := range batchIDsFromProps(snapshot.Summary.Properties) {
			if committed != "" {
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
		loaded, err = a.createTable(ctx, identifier, rows, ownerID, changefeed, cdcClusterID, upstreamID)
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

	schema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(arrowSchemaForRows(rows), false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	props := iceberggo.Properties{
		"format-version":       "2",
		"write.format.default": "parquet",
	}
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

	tbl, err := a.catalog.CreateTable(ctx, ident, schema, catalog.WithProperties(props))
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
