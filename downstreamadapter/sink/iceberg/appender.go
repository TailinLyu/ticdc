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

	tbl, err := a.loadOrCreateTable(ctx, identifier, rows)
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

func (a *icebergAppender) loadOrCreateTable(
	ctx context.Context,
	identifier []string,
	rows []map[string]any,
) (*icebergtable.Table, error) {
	key := identifierKey(identifier)

	a.mu.Lock()
	tbl := a.tables[key]
	a.mu.Unlock()
	if tbl != nil {
		return tbl, nil
	}

	loaded, err := a.catalog.LoadTable(ctx, icebergtable.Identifier(identifier))
	if err != nil {
		if !stderrors.Is(err, catalog.ErrNoSuchTable) {
			return nil, fmt.Errorf("load iceberg table %q: %w", identifier, err)
		}
		loaded, err = a.createTable(ctx, identifier, rows)
		if err != nil {
			return nil, errors.Trace(err)
		}
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
) (*icebergtable.Table, error) {
	ident := icebergtable.Identifier(identifier)
	namespace := catalog.NamespaceFromIdent(ident)
	if len(namespace) != 0 {
		err := a.catalog.CreateNamespace(ctx, namespace, iceberggo.Properties{})
		if err != nil && !stderrors.Is(err, catalog.ErrNamespaceAlreadyExists) {
			return nil, fmt.Errorf("create iceberg namespace %q: %w", namespace, err)
		}
	}

	schema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(arrowSchemaForRows(rows), false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tbl, err := a.catalog.CreateTable(ctx, ident, schema, catalog.WithProperties(iceberggo.Properties{
		"format-version":       "2",
		"write.format.default": "parquet",
	}))
	if err != nil {
		if stderrors.Is(err, catalog.ErrTableAlreadyExists) {
			return a.catalog.LoadTable(ctx, ident)
		}
		return nil, fmt.Errorf("create iceberg table %q: %w", identifier, err)
	}
	return tbl, nil
}
