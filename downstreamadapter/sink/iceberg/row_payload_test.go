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

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestBuildPayloadRowsForInsertUpdateDelete(t *testing.T) {
	tableInfo := newPayloadTestTableInfo()

	insertEvent := newPayloadTestEvent(tableInfo, 10, 20)
	insertEvent.RowTypes = []common.RowType{common.RowTypeInsert}
	insertEvent.Length = 1
	insertEvent.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	appendPayloadTestRow(insertEvent.Rows, 1, "alice")

	insertRows, err := buildPayloadRows(insertEvent)
	require.NoError(t, err)
	require.Len(t, insertRows, 1)
	require.Equal(t, "I", insertRows[0]["_op"])
	require.EqualValues(t, map[string]any{"id": int64(1), "name": "alice"}, insertRows[0]["data"])
	require.Nil(t, insertRows[0]["old"])

	updateEvent := newPayloadTestEvent(tableInfo, 30, 40)
	updateEvent.RowTypes = []common.RowType{common.RowTypeUpdate, common.RowTypeUpdate}
	updateEvent.Length = 1
	updateEvent.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 2)
	appendPayloadTestRow(updateEvent.Rows, 2, "before")
	appendPayloadTestRow(updateEvent.Rows, 2, "after")

	updateRows, err := buildPayloadRows(updateEvent)
	require.NoError(t, err)
	require.Len(t, updateRows, 1)
	require.Equal(t, "U", updateRows[0]["_op"])
	require.EqualValues(t, map[string]any{"id": int64(2), "name": "after"}, updateRows[0]["data"])
	require.EqualValues(t, map[string]any{"id": int64(2), "name": "before"}, updateRows[0]["old"])

	deleteEvent := newPayloadTestEvent(tableInfo, 50, 60)
	deleteEvent.RowTypes = []common.RowType{common.RowTypeDelete}
	deleteEvent.Length = 1
	deleteEvent.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	appendPayloadTestRow(deleteEvent.Rows, 3, "gone")

	deleteRows, err := buildPayloadRows(deleteEvent)
	require.NoError(t, err)
	require.Len(t, deleteRows, 1)
	require.Equal(t, "D", deleteRows[0]["_op"])
	require.Nil(t, deleteRows[0]["data"])
	require.EqualValues(t, map[string]any{"id": int64(3), "name": "gone"}, deleteRows[0]["old"])
}

func TestBuildPayloadRowsUsesStableStagingRowIDAcrossReplaySeq(t *testing.T) {
	tableInfo := newPayloadTestTableInfo()

	first := newPayloadTestEvent(tableInfo, 10, 20)
	first.Seq = 1
	first.RowTypes = []common.RowType{common.RowTypeInsert}
	first.Length = 1
	first.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	appendPayloadTestRow(first.Rows, 1, "alice")

	replayed := newPayloadTestEvent(tableInfo, 10, 20)
	replayed.Seq = 9
	replayed.RowTypes = []common.RowType{common.RowTypeInsert}
	replayed.Length = 1
	replayed.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	appendPayloadTestRow(replayed.Rows, 1, "alice")

	firstRows, err := buildPayloadRows(first)
	require.NoError(t, err)
	replayedRows, err := buildPayloadRows(replayed)
	require.NoError(t, err)
	require.Equal(t, firstRows[0][stagingRowIDField], replayedRows[0][stagingRowIDField])
}

func TestBuildPayloadRowsDistinguishesIdenticalRowsInSameEvent(t *testing.T) {
	tableInfo := newPayloadTestTableInfo()

	event := newPayloadTestEvent(tableInfo, 10, 20)
	event.RowTypes = []common.RowType{common.RowTypeInsert, common.RowTypeInsert}
	event.Length = 2
	event.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 2)
	appendPayloadTestRow(event.Rows, 1, "alice")
	appendPayloadTestRow(event.Rows, 1, "alice")

	rows, err := buildPayloadRows(event)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.NotEqual(t, rows[0][stagingRowIDField], rows[1][stagingRowIDField])
}

func newPayloadTestTableInfo() *common.TableInfo {
	idType := types.NewFieldType(mysql.TypeLong)
	idType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	nameType := types.NewFieldType(mysql.TypeVarchar)
	tableInfo := common.WrapTableInfo("app", &timodel.TableInfo{
		ID:         101,
		Name:       model.NewCIStr("users"),
		PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Offset:    0,
				Name:      model.NewCIStr("id"),
				State:     timodel.StatePublic,
				FieldType: *idType,
			},
			{
				ID:        2,
				Offset:    1,
				Name:      model.NewCIStr("name"),
				State:     timodel.StatePublic,
				FieldType: *nameType,
			},
		},
	})
	tableInfo.InitPrivateFields()
	return tableInfo
}

func newPayloadTestEvent(tableInfo *common.TableInfo, startTs uint64, commitTs uint64) *commonEvent.DMLEvent {
	return commonEvent.NewDMLEvent(common.NewDispatcherID(), tableInfo.TableName.TableID, startTs, commitTs, tableInfo)
}

func appendPayloadTestRow(chk *chunk.Chunk, id int64, name string) {
	chk.AppendInt64(0, id)
	chk.AppendString(1, name)
}
