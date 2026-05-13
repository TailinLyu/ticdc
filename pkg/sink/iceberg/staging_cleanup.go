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
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/pingcap/ticdc/pkg/errors"
)

type targetOwnerRecord struct {
	Changefeed string `json:"changefeed"`
}

// CleanupChangefeedStaging removes staged data files and target owner claims for
// a removed changefeed. It is intentionally independent of the sink runtime so
// coordinator-side remove cleanup can run even after the sink has already closed.
func CleanupChangefeedStaging(stagingDir string, changefeed string) error {
	if stagingDir == "" {
		return errors.New("iceberg staging dir is empty")
	}
	if changefeed == "" {
		return errors.New("iceberg changefeed is empty")
	}
	if err := os.RemoveAll(filepath.Join(stagingDir, pathSegment(changefeed))); err != nil {
		return errors.Trace(err)
	}
	return cleanupTargetOwnerClaims(stagingDir, changefeed)
}

func cleanupTargetOwnerClaims(stagingDir string, changefeed string) error {
	root := filepath.Join(stagingDir, ".owners")
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d == nil || d.IsDir() || filepath.Ext(path) != ".owner" {
			return nil
		}

		record, err := readTargetOwnerRecord(path)
		if err != nil {
			return err
		}
		if record.Changefeed != changefeed {
			return nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return errors.Trace(err)
	}
	return nil
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

func pathSegment(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
