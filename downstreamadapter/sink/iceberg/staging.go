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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
)

type stagedBatch struct {
	Changefeed  string             `json:"changefeed"`
	BatchID     string             `json:"batch_id"`
	Identifier  []string           `json:"identifier"`
	TableSchema *stagedTableSchema `json:"table_schema,omitempty"`
	Rows        []map[string]any   `json:"rows"`
	RowIDs      []string           `json:"row_ids,omitempty"`
	MaxCommitTs uint64             `json:"max_commit_ts"`
	CreatedAt   time.Time          `json:"created_at"`
}

type stagedFile struct {
	path  string
	batch stagedBatch
}

type targetOwnerRecord struct {
	Changefeed string    `json:"changefeed"`
	Identifier []string  `json:"identifier"`
	CreatedAt  time.Time `json:"created_at"`
}

type stageStore struct {
	root string
}

func newStageStore(root string) *stageStore {
	return &stageStore{root: root}
}

func (s *stageStore) Write(
	ctx context.Context,
	changefeed string,
	identifier []string,
	rows []map[string]any,
	maxCommitTs uint64,
	tableInfo ...*common.TableInfo,
) error {
	if len(rows) == 0 {
		return nil
	}
	if s.root == "" {
		return errors.New("iceberg staging dir is empty")
	}
	if err := ctx.Err(); err != nil {
		return errors.Trace(err)
	}

	dir := filepath.Join(s.root, pathSegment(changefeed), pathSegment(identifierKey(identifier)))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errors.Trace(err)
	}

	stagedRows, rowIDs, err := prepareStagedRows(identifier, rows)
	if err != nil {
		return errors.Trace(err)
	}
	batchID, err := stagedBatchID(identifier, stagedRows, rowIDs, maxCommitTs)
	if err != nil {
		return errors.Trace(err)
	}
	finalName := filepath.Join(dir, fmt.Sprintf("%020d-%s.json", maxCommitTs, batchID))
	if _, err := os.Stat(finalName); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return errors.Trace(err)
	}

	tmp, err := os.CreateTemp(dir, ".stage-*.tmp")
	if err != nil {
		return errors.Trace(err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	batch := stagedBatch{
		Changefeed:  changefeed,
		BatchID:     batchID,
		Identifier:  append([]string(nil), identifier...),
		TableSchema: firstStagedTableSchema(tableInfo),
		Rows:        stagedRows,
		RowIDs:      rowIDs,
		MaxCommitTs: maxCommitTs,
		CreatedAt:   time.Now().UTC(),
	}
	encoder := json.NewEncoder(tmp)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(&batch); err != nil {
		_ = tmp.Close()
		return errors.Trace(err)
	}
	if err := tmp.Close(); err != nil {
		return errors.Trace(err)
	}

	if err := os.Rename(tmpName, finalName); err != nil {
		return errors.Trace(err)
	}
	cleanup = false
	return nil
}

func firstStagedTableSchema(tableInfo []*common.TableInfo) *stagedTableSchema {
	if len(tableInfo) == 0 {
		return nil
	}
	return stagedTableSchemaForTableInfo(tableInfo[0])
}

func (s *stageStore) List(ctx context.Context, changefeed string) ([]stagedFile, error) {
	if s.root == "" {
		return nil, errors.New("iceberg staging dir is empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	root := filepath.Join(s.root, pathSegment(changefeed))
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}

	var files []stagedFile
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d == nil || d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		batch, err := readStagedBatch(path)
		if err != nil {
			return err
		}
		files = append(files, stagedFile{path: path, batch: batch})
		return ctx.Err()
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].batch.MaxCommitTs != files[j].batch.MaxCommitTs {
			return files[i].batch.MaxCommitTs < files[j].batch.MaxCommitTs
		}
		return files[i].path < files[j].path
	})
	return files, nil
}

func (s *stageStore) Delete(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return errors.Trace(err)
	}
	return nil
}

func (s *stageStore) ClaimTargetOwner(ctx context.Context, changefeed string, identifier []string) error {
	if s.root == "" {
		return errors.New("iceberg staging dir is empty")
	}
	if changefeed == "" {
		return errors.New("iceberg changefeed is empty")
	}
	if len(identifier) == 0 {
		return errors.New("iceberg target identifier is empty")
	}
	if err := ctx.Err(); err != nil {
		return errors.Trace(err)
	}

	dir := filepath.Join(s.root, ".owners")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errors.Trace(err)
	}
	path := filepath.Join(dir, pathSegment(identifierKey(identifier))+".owner")

	for {
		record, err := readTargetOwnerRecord(path)
		if err == nil {
			if record.Changefeed == changefeed {
				return nil
			}
			return errors.Annotatef(errIcebergTargetOwnerConflict,
				"owner %q conflicts with changefeed %q", record.Changefeed, changefeed)
		}
		if !os.IsNotExist(err) {
			return errors.Trace(err)
		}
		if err := ctx.Err(); err != nil {
			return errors.Trace(err)
		}

		record = targetOwnerRecord{
			Changefeed: changefeed,
			Identifier: append([]string(nil), identifier...),
			CreatedAt:  time.Now().UTC(),
		}
		payload, err := json.Marshal(&record)
		if err != nil {
			return errors.Trace(err)
		}
		payload = append(payload, '\n')

		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if os.IsExist(err) {
			continue
		}
		if err != nil {
			return errors.Trace(err)
		}
		if _, err := file.Write(payload); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return errors.Trace(err)
		}
		if err := file.Close(); err != nil {
			_ = os.Remove(path)
			return errors.Trace(err)
		}
		return nil
	}
}

func (s *stageStore) DeleteChangefeed(changefeed string) error {
	return icebergcfg.CleanupChangefeedStaging(s.root, changefeed)
}

func readTargetOwnerRecord(path string) (targetOwnerRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return targetOwnerRecord{}, err
	}
	defer file.Close()

	var record targetOwnerRecord
	if err := json.NewDecoder(file).Decode(&record); err != nil {
		return targetOwnerRecord{}, err
	}
	if record.Changefeed == "" {
		return targetOwnerRecord{}, errors.New("iceberg target owner changefeed is empty")
	}
	return record, nil
}

func readStagedBatch(path string) (stagedBatch, error) {
	file, err := os.Open(path)
	if err != nil {
		return stagedBatch{}, errors.Trace(err)
	}
	defer file.Close()

	var batch stagedBatch
	decoder := json.NewDecoder(file)
	decoder.UseNumber()
	if err := decoder.Decode(&batch); err != nil {
		return stagedBatch{}, errors.Trace(err)
	}
	return batch, nil
}

const stagingRowIDField = "_ticdc_iceberg_row_id"

func prepareStagedRows(identifier []string, rows []map[string]any) ([]map[string]any, []string, error) {
	stagedRows := make([]map[string]any, 0, len(rows))
	rowIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		stagedRow, rowID, err := prepareStagedRow(identifier, row)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		stagedRows = append(stagedRows, stagedRow)
		rowIDs = append(rowIDs, rowID)
	}
	return stagedRows, rowIDs, nil
}

func prepareStagedRow(identifier []string, row map[string]any) (map[string]any, string, error) {
	var rowID string
	if value, ok := row[stagingRowIDField]; ok {
		var isString bool
		rowID, isString = value.(string)
		if !isString || rowID == "" {
			return nil, "", errors.Errorf("invalid iceberg staging row id %v", value)
		}
	}

	stagedRow := make(map[string]any, len(row))
	for key, value := range row {
		if key == stagingRowIDField {
			continue
		}
		stagedRow[key] = value
	}
	if rowID == "" {
		var err error
		rowID, err = stagedRowID(identifier, stagedRow)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
	}
	return stagedRow, rowID, nil
}

func stagedRowIDs(identifier []string, rows []map[string]any, rowIDs []string) ([]string, error) {
	if len(rowIDs) > 0 {
		if len(rowIDs) != len(rows) {
			return nil, errors.Errorf("iceberg staged row id count %d does not match row count %d",
				len(rowIDs), len(rows))
		}
		copied := make([]string, 0, len(rowIDs))
		for _, rowID := range rowIDs {
			if rowID == "" {
				return nil, errors.New("iceberg staged row id is empty")
			}
			copied = append(copied, rowID)
		}
		return copied, nil
	}

	computed := make([]string, 0, len(rows))
	for _, row := range rows {
		rowID, err := stagedRowID(identifier, row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		computed = append(computed, rowID)
	}
	return computed, nil
}

func pathSegment(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func stagedBatchID(identifier []string, rows []map[string]any, rowIDs []string, maxCommitTs uint64) (string, error) {
	payload := struct {
		Identifier  []string `json:"identifier"`
		RowIDs      []string `json:"row_ids"`
		MaxCommitTs uint64   `json:"max_commit_ts"`
	}{
		Identifier:  identifier,
		RowIDs:      rowIDs,
		MaxCommitTs: maxCommitTs,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Trace(err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func stagedRowID(identifier []string, row map[string]any) (string, error) {
	payload := struct {
		Identifier []string       `json:"identifier"`
		Row        map[string]any `json:"row"`
	}{
		Identifier: identifier,
		Row:        stagingRowIdentityPayload(row),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Trace(err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func stagingRowIdentityPayload(row map[string]any) map[string]any {
	identity := make(map[string]any, len(row))
	for key, value := range row {
		switch key {
		case stagingRowIDField, "_seq":
			continue
		default:
			identity[key] = value
		}
	}
	return identity
}

func stagingRowIDForEvent(
	tableID int64,
	startTs uint64,
	commitTs uint64,
	rowIndex int,
	rowKey []byte,
	row map[string]any,
) (string, error) {
	var rowIndexValue *int
	if len(rowKey) == 0 {
		rowIndexValue = &rowIndex
	}
	payload := struct {
		TableID  int64          `json:"table_id"`
		StartTs  uint64         `json:"start_ts"`
		CommitTs uint64         `json:"commit_ts"`
		RowIndex *int           `json:"row_index,omitempty"`
		RowKey   []byte         `json:"row_key,omitempty"`
		Row      map[string]any `json:"row"`
	}{
		TableID:  tableID,
		StartTs:  startTs,
		CommitTs: commitTs,
		RowIndex: rowIndexValue,
		RowKey:   rowKey,
		Row:      stagingRowIdentityPayload(row),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Trace(err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
