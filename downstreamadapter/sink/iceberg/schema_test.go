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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	iceberggo "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestArrowSchemaForRows(t *testing.T) {
	schema := arrowSchemaForRows([]map[string]any{
		{
			"_commit_ts": int64(42),
			"data": map[string]any{
				"id":     int64(1),
				"active": true,
				"score":  json.Number("12.5"),
				"name":   "alice",
			},
			"old": nil,
		},
		{
			"_commit_ts": int64(43),
			"data":       nil,
			"old": map[string]any{
				"id":     int64(1),
				"active": false,
				"name":   "alice",
			},
		},
	})

	require.Equal(t, arrow.PrimitiveTypes.Int64, schema.Field(0).Type)
	dataType := schema.Field(8).Type.(*arrow.StructType)
	oldType := schema.Field(9).Type.(*arrow.StructType)
	require.Equal(t, []arrow.Field{
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, dataType.Fields())
	require.Equal(t, dataType.Fields(), oldType.Fields())
}

func TestArrowSchemaForRowsUsesDataFieldsForOldStruct(t *testing.T) {
	schema := arrowSchemaForRows([]map[string]any{
		{
			"_commit_ts": int64(42),
			"data":       map[string]any{"id": int64(1), "name": "alice"},
			"old":        nil,
		},
	})

	dataType := schema.Field(8).Type.(*arrow.StructType)
	oldType := schema.Field(9).Type.(*arrow.StructType)
	require.Equal(t, []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, dataType.Fields())
	require.Equal(t, dataType.Fields(), oldType.Fields())
}

func TestArrowSchemaForTableInfoIncludesColumnsWithoutObservedValues(t *testing.T) {
	schema := arrowSchemaForTableInfo(newPayloadTestTableInfo())

	dataType := schema.Field(8).Type.(*arrow.StructType)
	oldType := schema.Field(9).Type.(*arrow.StructType)
	require.Equal(t, []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, dataType.Fields())
	require.Equal(t, dataType.Fields(), oldType.Fields())
}

func TestCDCLogPartitionSpecUsesDtHrIdentity(t *testing.T) {
	icebergSchema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(
		arrowSchemaForTableInfo(newPayloadTestTableInfo()), false)
	require.NoError(t, err)

	spec, err := cdcLogPartitionSpec(icebergSchema)
	require.NoError(t, err)

	require.Equal(t, 2, spec.NumFields())
	require.Equal(t, "dt", spec.Field(0).Name)
	require.Equal(t, iceberggo.IdentityTransform{}, spec.Field(0).Transform)
	require.Equal(t, "hr", spec.Field(1).Name)
	require.Equal(t, iceberggo.IdentityTransform{}, spec.Field(1).Transform)
}
