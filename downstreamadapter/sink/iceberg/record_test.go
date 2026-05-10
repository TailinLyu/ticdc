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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

func TestRowsToRecordBatch(t *testing.T) {
	dataType := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "_op", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "_commit_ts", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: dataType, Nullable: true},
		{Name: "old", Type: dataType, Nullable: true},
	}, nil)

	record, err := rowsToRecordBatch([]map[string]any{
		{
			"_op":        "I",
			"_commit_ts": int64(42),
			"data":       map[string]any{"id": int64(1), "name": "alice"},
			"old":        nil,
		},
	}, schema)
	require.NoError(t, err)
	defer record.Release()

	require.EqualValues(t, 1, record.NumRows())
	require.EqualValues(t, 4, record.NumCols())
	require.Equal(t, schema, record.Schema())
}

func TestRowsToRecordBatchAcceptsRichCDCColumnTypes(t *testing.T) {
	dataType := arrow.StructOf(
		arrow.Field{Name: "amount", Type: &arrow.Decimal128Type{Precision: 20, Scale: 4}, Nullable: true},
		arrow.Field{Name: "created_date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		arrow.Field{Name: "event_time", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		arrow.Field{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		arrow.Field{Name: "duration", Type: &arrow.Time64Type{Unit: arrow.Microsecond}, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "_op", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "_commit_dt", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "data", Type: dataType, Nullable: true},
	}, nil)

	record, err := rowsToRecordBatch([]map[string]any{
		{
			"_op":        "I",
			"_commit_dt": "2026-05-10T12:34:56.123456Z",
			"data": map[string]any{
				"amount":       "123456789012.3456",
				"created_date": "2026-05-10",
				"event_time":   "2026-05-10 12:34:56.123456",
				"created_at":   "2026-05-10T12:34:56.123456Z",
				"duration":     int64(3723000456),
			},
		},
	}, schema)
	require.NoError(t, err)
	defer record.Release()

	require.EqualValues(t, 1, record.NumRows())
	data := record.Column(2).(*array.Struct)
	require.False(t, data.IsNull(0))
	require.Equal(t, "123456789012.3456", data.Field(0).ValueStr(0))
	require.Equal(t, "2026-05-10", data.Field(1).ValueStr(0))
}
