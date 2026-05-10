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
	"slices"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	iceberggo "github.com/apache/iceberg-go"
	"github.com/pingcap/ticdc/pkg/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type stagedColumnType string

const (
	stagedColumnTypeBinary  stagedColumnType = "binary"
	stagedColumnTypeBool    stagedColumnType = "bool"
	stagedColumnTypeFloat64 stagedColumnType = "float64"
	stagedColumnTypeInt64   stagedColumnType = "int64"
	stagedColumnTypeString  stagedColumnType = "string"
)

type stagedColumnSchema struct {
	Name string           `json:"name"`
	Type stagedColumnType `json:"type"`
}

type stagedTableSchema struct {
	Columns []stagedColumnSchema `json:"columns"`
}

func arrowSchemaForRows(rows []map[string]any) *arrow.Schema {
	rowType := rowStructType(rows, "data", "old")
	return cdcLogArrowSchema(rowType)
}

func arrowSchemaForTableInfo(tableInfo *common.TableInfo) *arrow.Schema {
	return arrowSchemaForStagedTableSchema(stagedTableSchemaForTableInfo(tableInfo))
}

func arrowSchemaForStagedTableSchema(tableSchema *stagedTableSchema) *arrow.Schema {
	rowType := rowStructTypeForTableSchema(tableSchema)
	return cdcLogArrowSchema(rowType)
}

func cdcLogArrowSchema(rowType arrow.DataType) *arrow.Schema {
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

func stagedTableSchemaForTableInfo(tableInfo *common.TableInfo) *stagedTableSchema {
	if tableInfo == nil {
		return nil
	}
	columns := make([]stagedColumnSchema, 0, len(tableInfo.GetColumns()))
	for _, col := range tableInfo.GetColumns() {
		if col.IsVirtualGenerated() {
			continue
		}
		columns = append(columns, stagedColumnSchema{
			Name: col.Name.O,
			Type: stagedColumnTypeForColumn(col),
		})
	}
	return &stagedTableSchema{Columns: columns}
}

func sameStagedTableSchema(left *stagedTableSchema, right *stagedTableSchema) bool {
	switch {
	case left == nil && right == nil:
		return true
	case left == nil || right == nil:
		return false
	default:
		return slices.Equal(left.Columns, right.Columns)
	}
}

func rowStructTypeForTableSchema(tableSchema *stagedTableSchema) arrow.DataType {
	if tableSchema == nil {
		return arrow.StructOf()
	}
	fields := make([]arrow.Field, 0, len(tableSchema.Columns))
	for _, col := range tableSchema.Columns {
		fields = append(fields, arrow.Field{Name: col.Name, Type: arrowTypeForStagedColumn(col.Type), Nullable: true})
	}
	return arrow.StructOf(fields...)
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

func stagedColumnTypeForColumn(col *timodel.ColumnInfo) stagedColumnType {
	switch col.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if mysql.HasBinaryFlag(col.GetFlag()) {
			return stagedColumnTypeBinary
		}
		return stagedColumnTypeString
	case mysql.TypeFloat, mysql.TypeDouble:
		return stagedColumnTypeFloat64
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeYear, mysql.TypeBit:
		return stagedColumnTypeInt64
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp,
		mysql.TypeDuration, mysql.TypeEnum, mysql.TypeSet, mysql.TypeNewDecimal,
		mysql.TypeJSON, mysql.TypeTiDBVectorFloat32:
		return stagedColumnTypeString
	default:
		return stagedColumnTypeString
	}
}

func arrowTypeForStagedColumn(columnType stagedColumnType) arrow.DataType {
	switch columnType {
	case stagedColumnTypeBinary:
		return arrow.BinaryTypes.Binary
	case stagedColumnTypeBool:
		return arrow.FixedWidthTypes.Boolean
	case stagedColumnTypeFloat64:
		return arrow.PrimitiveTypes.Float64
	case stagedColumnTypeInt64:
		return arrow.PrimitiveTypes.Int64
	case stagedColumnTypeString:
		return arrow.BinaryTypes.String
	default:
		return arrow.BinaryTypes.String
	}
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

func cdcLogPartitionSpec(schema *iceberggo.Schema) (*iceberggo.PartitionSpec, error) {
	spec, err := iceberggo.NewPartitionSpecOpts(
		iceberggo.AddPartitionFieldByName("dt", "dt", iceberggo.IdentityTransform{}, schema, nil),
		iceberggo.AddPartitionFieldByName("hr", "hr", iceberggo.IdentityTransform{}, schema, nil),
	)
	if err != nil {
		return nil, err
	}
	return &spec, nil
}
