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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
)

func main() {
	catalogURI := flag.String("catalog", "http://127.0.0.1:8181/", "Iceberg REST catalog URI")
	warehouse := flag.String("warehouse", "file:///tmp/iceberg-warehouse", "Iceberg warehouse location")
	namespace := flag.String("namespace", "ticdc_iceberg_e2e", "Iceberg namespace")
	tableName := flag.String("table", "orders_cdc", "Iceberg table")
	summary := flag.Bool("summary", false, "print row and operation counts instead of every row")
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

	_, records, err := tbl.Scan().ToArrowRecords(ctx)
	if err != nil {
		fatalf("scan table %s.%s: %v", *namespace, *tableName, err)
	}

	var rows int64
	opCounts := map[string]int64{}
	for rec, err := range records {
		if err != nil {
			fatalf("read record batch: %v", err)
		}
		if *summary {
			countOps(rec, opCounts)
		} else {
			if err := array.RecordToJSON(rec, os.Stdout); err != nil {
				rec.Release()
				fatalf("encode record batch: %v", err)
			}
		}
		rows += rec.NumRows()
		rec.Release()
	}
	if *summary {
		fmt.Printf("rows=%d inserts=%d updates=%d deletes=%d\n", rows, opCounts["I"], opCounts["U"], opCounts["D"])
		return
	}
	fmt.Fprintf(os.Stderr, "rows=%d\n", rows)
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
