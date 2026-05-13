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
	"github.com/pingcap/ticdc/pkg/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	parsermodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
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

func TestArrowSchemaForTableInfoUsesDesignRichTypes(t *testing.T) {
	tableInfo := newRichPayloadTestTableInfo()

	schema := arrowSchemaForTableInfo(tableInfo)

	require.True(t, arrow.TypeEqual(
		&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
		schema.Field(1).Type))
	dataType := schema.Field(8).Type.(*arrow.StructType)
	fields := dataType.Fields()
	require.Len(t, fields, 7)
	require.Equal(t, "u64", fields[0].Name)
	require.True(t, arrow.TypeEqual(arrow.BinaryTypes.String, fields[0].Type))
	require.Equal(t, "amount", fields[1].Name)
	require.True(t, arrow.TypeEqual(&arrow.Decimal128Type{Precision: 20, Scale: 4}, fields[1].Type))
	require.Equal(t, "created_date", fields[2].Name)
	require.True(t, arrow.TypeEqual(arrow.FixedWidthTypes.Date32, fields[2].Type))
	require.Equal(t, "event_time", fields[3].Name)
	require.True(t, arrow.TypeEqual(&arrow.TimestampType{Unit: arrow.Microsecond}, fields[3].Type))
	require.Equal(t, "created_at", fields[4].Name)
	require.True(t, arrow.TypeEqual(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, fields[4].Type))
	require.Equal(t, "duration", fields[5].Name)
	require.True(t, arrow.TypeEqual(&arrow.Time64Type{Unit: arrow.Microsecond}, fields[5].Type))
	require.Equal(t, "wide_bit", fields[6].Name)
	require.True(t, arrow.TypeEqual(arrow.BinaryTypes.Binary, fields[6].Type))
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

func newRichPayloadTestTableInfo() *common.TableInfo {
	unsignedBigint := types.NewFieldType(mysql.TypeLonglong)
	unsignedBigint.AddFlag(mysql.UnsignedFlag)

	decimalType := types.NewFieldType(mysql.TypeNewDecimal)
	decimalType.SetFlen(20)
	decimalType.SetDecimal(4)

	dateType := types.NewFieldType(mysql.TypeDate)
	datetimeType := types.NewFieldType(mysql.TypeDatetime)
	timestampType := types.NewFieldType(mysql.TypeTimestamp)

	durationType := types.NewFieldType(mysql.TypeDuration)
	durationType.SetDecimal(6)

	bitType := types.NewFieldType(mysql.TypeBit)
	bitType.SetFlen(64)

	tableInfo := common.WrapTableInfo("app", &timodel.TableInfo{
		ID:   202,
		Name: parsermodel.NewCIStr("rich_types"),
		Columns: []*timodel.ColumnInfo{
			newColumnInfoForSchemaTest(1, "u64", unsignedBigint),
			newColumnInfoForSchemaTest(2, "amount", decimalType),
			newColumnInfoForSchemaTest(3, "created_date", dateType),
			newColumnInfoForSchemaTest(4, "event_time", datetimeType),
			newColumnInfoForSchemaTest(5, "created_at", timestampType),
			newColumnInfoForSchemaTest(6, "duration", durationType),
			newColumnInfoForSchemaTest(7, "wide_bit", bitType),
		},
	})
	tableInfo.InitPrivateFields()
	return tableInfo
}

func newColumnInfoForSchemaTest(id int64, name string, fieldType *types.FieldType) *timodel.ColumnInfo {
	return &timodel.ColumnInfo{
		ID:        id,
		Offset:    int(id - 1),
		Name:      parsermodel.NewCIStr(name),
		State:     timodel.StatePublic,
		FieldType: *fieldType,
	}
}
