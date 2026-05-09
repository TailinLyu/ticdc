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
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/pingcap/ticdc/pkg/errors"
)

type stagedBatch struct {
	Changefeed  string           `json:"changefeed"`
	Identifier  []string         `json:"identifier"`
	Rows        []map[string]any `json:"rows"`
	MaxCommitTs uint64           `json:"max_commit_ts"`
	CreatedAt   time.Time        `json:"created_at"`
}

type stagedFile struct {
	path  string
	batch stagedBatch
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
		Identifier:  append([]string(nil), identifier...),
		Rows:        rows,
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

	finalName := filepath.Join(
		dir,
		fmt.Sprintf("%020d-%d-%s.json", maxCommitTs, time.Now().UnixNano(), randomStageSuffix()),
	)
	if err := os.Rename(tmpName, finalName); err != nil {
		return errors.Trace(err)
	}
	cleanup = false
	return nil
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

func pathSegment(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func randomStageSuffix() string {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes[:])
}
