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
	"bytes"
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pingcap/ticdc/pkg/errors"
)

func rowsToRecordBatch(rows []map[string]any, schema *arrow.Schema) (arrow.RecordBatch, error) {
	payload, err := json.Marshal(rows)
	if err != nil {
		return nil, errors.Trace(err)
	}
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(payload))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return record, nil
}
