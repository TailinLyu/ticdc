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
	"encoding/json"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

func arrowSchemaForRows(rows []map[string]any) *arrow.Schema {
	rowType := rowStructType(rows, "data", "old")

	return arrow.NewSchema([]arrow.Field{
		{Name: "_commit_ts", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "_commit_dt", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "_start_ts", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "_seq", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "_table_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "dt", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "hr", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "_op", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "data", Type: rowType, Nullable: true},
		{Name: "old", Type: rowType, Nullable: true},
	}, nil)
}

func rowStructType(rows []map[string]any, fieldNames ...string) arrow.DataType {
	typesByName := make(map[string]arrow.DataType)
	for _, row := range rows {
		for _, fieldName := range fieldNames {
			values, ok := row[fieldName].(map[string]any)
			if !ok {
				continue
			}
			for name, value := range values {
				if _, ok := typesByName[name]; !ok && value != nil {
					typesByName[name] = arrowTypeForValue(value)
				}
			}
		}
	}

	names := make([]string, 0, len(typesByName))
	for name := range typesByName {
		names = append(names, name)
	}
	sort.Strings(names)

	fields := make([]arrow.Field, 0, len(names))
	for _, name := range names {
		fields = append(fields, arrow.Field{Name: name, Type: typesByName[name], Nullable: true})
	}
	return arrow.StructOf(fields...)
}

func arrowTypeForValue(value any) arrow.DataType {
	switch value.(type) {
	case bool:
		return arrow.FixedWidthTypes.Boolean
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return arrow.PrimitiveTypes.Int64
	case float32, float64:
		return arrow.PrimitiveTypes.Float64
	case json.Number:
		number := value.(json.Number).String()
		if strings.ContainsAny(number, ".eE") {
			return arrow.PrimitiveTypes.Float64
		}
		if _, err := value.(json.Number).Int64(); err == nil {
			return arrow.PrimitiveTypes.Int64
		}
		return arrow.PrimitiveTypes.Float64
	case []byte:
		return arrow.BinaryTypes.Binary
	default:
		return arrow.BinaryTypes.String
	}
}
