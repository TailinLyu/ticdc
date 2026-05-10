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
	"encoding/hex"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func buildPayloadRows(event *commonEvent.DMLEvent) ([]map[string]any, error) {
	if event == nil || event.TableInfo == nil {
		return nil, errors.New("iceberg sink received DML event without table info")
	}

	event.Rewind()
	defer event.Rewind()

	rows := make([]map[string]any, 0, event.Len())
	for {
		row, ok := event.GetNextRow()
		if !ok {
			break
		}

		payload := map[string]any{
			"_commit_ts": int64(event.CommitTs),
			"_commit_dt": commitTime(event.CommitTs).Format(time.RFC3339Nano),
			"_start_ts":  int64(event.StartTs),
			"_seq":       int64(event.Seq),
			"_table_id":  event.PhysicalTableID,
			"dt":         commitTime(event.CommitTs).Format("2006-01-02"),
			"hr":         commitTime(event.CommitTs).Format("15"),
		}

		switch row.RowType {
		case common.RowTypeInsert:
			data, err := rowToPayloadMap(&row.Row, event.TableInfo)
			if err != nil {
				return nil, err
			}
			payload["_op"] = "I"
			payload["data"] = data
			payload["old"] = nil
		case common.RowTypeUpdate:
			data, err := rowToPayloadMap(&row.Row, event.TableInfo)
			if err != nil {
				return nil, err
			}
			old, err := rowToPayloadMap(&row.PreRow, event.TableInfo)
			if err != nil {
				return nil, err
			}
			payload["_op"] = "U"
			payload["data"] = data
			payload["old"] = old
		case common.RowTypeDelete:
			old, err := rowToPayloadMap(&row.PreRow, event.TableInfo)
			if err != nil {
				return nil, err
			}
			payload["_op"] = "D"
			payload["data"] = nil
			payload["old"] = old
		default:
			return nil, errors.Errorf("unsupported iceberg row type %d", row.RowType)
		}
		rowID, err := stagingRowIDForEvent(
			event.PhysicalTableID,
			event.StartTs,
			event.CommitTs,
			len(rows),
			row.RowKey,
			payload)
		if err != nil {
			return nil, errors.Trace(err)
		}
		payload[stagingRowIDField] = rowID
		rows = append(rows, payload)
	}
	return rows, nil
}

func rowToPayloadMap(row *chunk.Row, tableInfo *common.TableInfo) (map[string]any, error) {
	values := make(map[string]any, len(tableInfo.GetColumns()))
	for idx, col := range tableInfo.GetColumns() {
		if col.IsVirtualGenerated() {
			continue
		}
		value, err := columnValue(row, idx, col)
		if err != nil {
			return nil, err
		}
		values[col.Name.O] = value
	}
	return values, nil
}

func columnValue(row *chunk.Row, idx int, col *timodel.ColumnInfo) (any, error) {
	if row.IsNull(idx) {
		return nil, nil
	}

	switch col.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		if mysql.HasBinaryFlag(col.GetFlag()) {
			return row.GetBytes(idx), nil
		}
		return row.GetString(idx), nil
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if mysql.HasBinaryFlag(col.GetFlag()) {
			return row.GetBytes(idx), nil
		}
		return row.GetString(idx), nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		return row.GetTime(idx).String(), nil
	case mysql.TypeDuration:
		return row.GetDuration(idx, col.GetDecimal()).String(), nil
	case mysql.TypeEnum:
		enumValue := row.GetEnum(idx).Value
		enumVar, err := types.ParseEnumValue(col.GetElems(), enumValue)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return enumVar.Name, nil
	case mysql.TypeSet:
		bitValue := row.GetEnum(idx).Value
		setVar, err := types.ParseSetValue(col.GetElems(), bitValue)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return setVar.Name, nil
	case mysql.TypeBit:
		d := row.GetDatum(idx, &col.FieldType)
		value, err := d.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return value, nil
	case mysql.TypeNewDecimal:
		return row.GetMyDecimal(idx).String(), nil
	case mysql.TypeJSON:
		return row.GetJSON(idx).String(), nil
	case mysql.TypeTiDBVectorFloat32:
		return row.GetVectorFloat32(idx).String(), nil
	default:
		d := row.GetDatum(idx, &col.FieldType)
		value := d.GetValue()
		if bytes, ok := value.([]byte); ok {
			return hex.EncodeToString(bytes), nil
		}
		return value, nil
	}
}

func commitTime(ts uint64) time.Time {
	return time.UnixMilli(int64(ts >> 18)).UTC()
}
