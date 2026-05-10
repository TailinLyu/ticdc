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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCleanupChangefeedStagingDeletesOnlyTargetChangefeed(t *testing.T) {
	root := t.TempDir()
	changefeed := "default/iceberg-left"
	otherChangefeed := "default/iceberg-right"

	ownStageDir := filepath.Join(root, pathSegment(changefeed), pathSegment("db/orders_cdc"))
	otherStageDir := filepath.Join(root, pathSegment(otherChangefeed), pathSegment("db/orders_cdc"))
	require.NoError(t, os.MkdirAll(ownStageDir, 0o755))
	require.NoError(t, os.MkdirAll(otherStageDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(ownStageDir, "00000000000000000001-a.json"), []byte("{}\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(otherStageDir, "00000000000000000002-b.json"), []byte("{}\n"), 0o644))

	ownerDir := filepath.Join(root, ".owners")
	require.NoError(t, os.MkdirAll(ownerDir, 0o755))
	ownOwner := filepath.Join(ownerDir, pathSegment("db/orders_cdc")+".owner")
	otherOwner := filepath.Join(ownerDir, pathSegment("db/customers_cdc")+".owner")
	require.NoError(t, os.WriteFile(ownOwner, []byte(`{"changefeed":"default/iceberg-left"}`+"\n"), 0o644))
	require.NoError(t, os.WriteFile(otherOwner, []byte(`{"changefeed":"default/iceberg-right"}`+"\n"), 0o644))

	require.NoError(t, CleanupChangefeedStaging(root, changefeed))

	_, err := os.Stat(filepath.Join(root, pathSegment(changefeed)))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(ownOwner)
	require.True(t, os.IsNotExist(err))
	require.FileExists(t, filepath.Join(otherStageDir, "00000000000000000002-b.json"))
	require.FileExists(t, otherOwner)
}

func TestCleanupChangefeedStagingAllowsMissingOwnerDirectory(t *testing.T) {
	root := t.TempDir()
	changefeed := "default/iceberg-left"
	require.NoError(t, os.MkdirAll(filepath.Join(root, pathSegment(changefeed)), 0o755))

	require.NoError(t, CleanupChangefeedStaging(root, changefeed))

	_, err := os.Stat(filepath.Join(root, pathSegment(changefeed)))
	require.True(t, os.IsNotExist(err))
}
