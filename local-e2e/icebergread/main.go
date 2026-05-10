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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
)

func main() {
	catalogURI := flag.String("catalog", "http://127.0.0.1:8181/", "Iceberg REST catalog URI")
	warehouse := flag.String("warehouse", "file:///tmp/iceberg-warehouse", "Iceberg warehouse location")
	namespace := flag.String("namespace", "ticdc_iceberg_e2e", "Iceberg namespace")
	tableName := flag.String("table", "orders_cdc", "Iceberg table")
	summary := flag.Bool("summary", false, "print row and operation counts instead of every row")
	schemaOnly := flag.Bool("schema", false, "print Arrow schema paths and exit")
	requireInsertIDsRaw := flag.String("require-insert-ids", "", "comma-separated data.id values that must appear in insert rows")
	flag.Parse()

	ctx := context.Background()
	cat, err := rest.NewCatalog(ctx, "ticdc-readback", *catalogURI, rest.WithWarehouseLocation(*warehouse))
	if err != nil {
		fatalf("create REST catalog: %v", err)
	}

	tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(*namespace, *tableName))
	if err != nil {
		fatalf("load table %s.%s: %v", *namespace, *tableName, err)
	}
	if *schemaOnly {
		arrowSchema, err := icebergtable.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
		if err != nil {
			fatalf("convert schema: %v", err)
		}
		printSchema("", arrowSchema.Fields())
		return
	}

	_, records, err := tbl.Scan().ToArrowRecords(ctx)
	if err != nil {
		fatalf("scan table %s.%s: %v", *namespace, *tableName, err)
	}

	var rows int64
	opCounts := map[string]int64{}
	requiredInsertIDs := parseRequiredIDs(*requireInsertIDsRaw)
	foundInsertIDs := make(map[string]struct{}, len(requiredInsertIDs))
	for rec, err := range records {
		if err != nil {
			fatalf("read record batch: %v", err)
		}
		if *summary {
			countOps(rec, opCounts)
		} else if len(requiredInsertIDs) == 0 {
			if err := array.RecordToJSON(rec, os.Stdout); err != nil {
				rec.Release()
				fatalf("encode record batch: %v", err)
			}
		}
		if len(requiredInsertIDs) > 0 {
			collectInsertIDs(rec, requiredInsertIDs, foundInsertIDs)
		}
		rows += rec.NumRows()
		rec.Release()
	}
	if len(requiredInsertIDs) > 0 {
		missing := missingIDs(requiredInsertIDs, foundInsertIDs)
		if len(missing) > 0 {
			fatalf("missing required insert ids: %s", strings.Join(missing, ","))
		}
		fmt.Printf("required_insert_ids=ok count=%d\n", len(requiredInsertIDs))
		return
	}
	if *summary {
		fmt.Printf("rows=%d inserts=%d updates=%d deletes=%d\n", rows, opCounts["I"], opCounts["U"], opCounts["D"])
		return
	}
	fmt.Fprintf(os.Stderr, "rows=%d\n", rows)
}

func printSchema(prefix string, fields []arrow.Field) {
	for _, field := range fields {
		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}
		fmt.Printf("%s\t%s\n", path, field.Type)
		if structType, ok := field.Type.(*arrow.StructType); ok {
			printSchema(path, structType.Fields())
		}
	}
}

func parseRequiredIDs(raw string) map[string]struct{} {
	values := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			values[part] = struct{}{}
		}
	}
	return values
}

func collectInsertIDs(rec arrow.Record, required map[string]struct{}, found map[string]struct{}) {
	opIndices := rec.Schema().FieldIndices("_op")
	dataIndices := rec.Schema().FieldIndices("data")
	if len(opIndices) == 0 || len(dataIndices) == 0 {
		return
	}
	ops, ok := rec.Column(opIndices[0]).(*array.String)
	if !ok {
		return
	}
	data, ok := rec.Column(dataIndices[0]).(*array.Struct)
	if !ok {
		return
	}
	dataType, ok := data.DataType().(*arrow.StructType)
	if !ok {
		return
	}
	idIndex, ok := dataType.FieldIdx("id")
	if !ok {
		return
	}
	idColumn := data.Field(idIndex)
	for idx := 0; idx < int(rec.NumRows()); idx++ {
		if ops.IsNull(idx) || ops.Value(idx) != "I" || data.IsNull(idx) {
			continue
		}
		id, ok := arrayValueString(idColumn, idx)
		if !ok {
			continue
		}
		if _, needed := required[id]; needed {
			found[id] = struct{}{}
		}
	}
}

func arrayValueString(values arrow.Array, idx int) (string, bool) {
	if values.IsNull(idx) {
		return "", false
	}
	switch column := values.(type) {
	case *array.Int64:
		return strconv.FormatInt(column.Value(idx), 10), true
	case *array.Int32:
		return strconv.FormatInt(int64(column.Value(idx)), 10), true
	case *array.String:
		return column.Value(idx), true
	default:
		return "", false
	}
}

func missingIDs(required map[string]struct{}, found map[string]struct{}) []string {
	missing := make([]string, 0)
	for id := range required {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

func countOps(rec arrow.Record, counts map[string]int64) {
	indices := rec.Schema().FieldIndices("_op")
	if len(indices) == 0 {
		return
	}
	column, ok := rec.Column(indices[0]).(*array.String)
	if !ok {
		return
	}
	for idx := 0; idx < column.Len(); idx++ {
		if column.IsNull(idx) {
			continue
		}
		counts[column.Value(idx)]++
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
