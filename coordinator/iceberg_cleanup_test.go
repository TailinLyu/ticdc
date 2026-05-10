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

package coordinator

import (
	"context"
	"encoding/base64"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/stretchr/testify/require"
)

func TestCleanupRemovedChangefeedSinkArtifactsRemovesIcebergStaging(t *testing.T) {
	stagingDir := t.TempDir()
	id := common.NewChangeFeedIDWithName("iceberg-left", common.DefaultKeyspaceName)
	otherID := common.NewChangeFeedIDWithName("iceberg-right", common.DefaultKeyspaceName)

	ownStageDir := filepath.Join(stagingDir, coordinatorTestPathSegment(id.String()), coordinatorTestPathSegment("db/orders_cdc"))
	otherStageDir := filepath.Join(stagingDir, coordinatorTestPathSegment(otherID.String()), coordinatorTestPathSegment("db/orders_cdc"))
	require.NoError(t, os.MkdirAll(ownStageDir, 0o755))
	require.NoError(t, os.MkdirAll(otherStageDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(ownStageDir, "00000000000000000001-a.json"), []byte("{}\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(otherStageDir, "00000000000000000002-b.json"), []byte("{}\n"), 0o644))

	ownerDir := filepath.Join(stagingDir, ".owners")
	require.NoError(t, os.MkdirAll(ownerDir, 0o755))
	ownOwner := filepath.Join(ownerDir, coordinatorTestPathSegment("db/orders_cdc")+".owner")
	otherOwner := filepath.Join(ownerDir, coordinatorTestPathSegment("db/customers_cdc")+".owner")
	require.NoError(t, os.WriteFile(ownOwner, []byte(`{"changefeed":"`+id.String()+`"}`+"\n"), 0o644))
	require.NoError(t, os.WriteFile(otherOwner, []byte(`{"changefeed":"`+otherID.String()+`"}`+"\n"), 0o644))

	icebergURI := url.URL{Scheme: "iceberg", Host: "localhost:8181"}
	query := icebergURI.Query()
	query.Set("warehouse", "file:///tmp/iceberg-warehouse")
	query.Set("staging-dir", (&url.URL{Scheme: "file", Path: stagingDir}).String())
	icebergURI.RawQuery = query.Encode()

	cleanupRemovedChangefeedSinkArtifacts(id, icebergURI.String(), 0)

	_, err := os.Stat(filepath.Join(stagingDir, coordinatorTestPathSegment(id.String())))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(ownOwner)
	require.True(t, os.IsNotExist(err))
	require.FileExists(t, filepath.Join(otherStageDir, "00000000000000000002-b.json"))
	require.FileExists(t, otherOwner)
}

func TestCleanupRemovedChangefeedSinkArtifactsRemovesIcebergWarehouseOwnerClaim(t *testing.T) {
	ctx := context.Background()
	stagingDir := t.TempDir()
	warehouse := (&url.URL{Scheme: "file", Path: t.TempDir()}).String()
	id := common.NewChangeFeedIDWithName("iceberg-left", common.DefaultKeyspaceName)
	upstreamID := uint64(1001)
	identifier := []string{"test", "orders_cdc"}
	owner := icebergcfg.NewTargetOwnerClaim(
		config.GetGlobalServerConfig().ClusterID,
		upstreamID,
		id.String(),
		identifier)
	require.NoError(t, icebergcfg.ClaimTargetOwner(ctx, warehouse, owner))

	icebergURI := url.URL{Scheme: "iceberg", Host: "localhost:8181"}
	query := icebergURI.Query()
	query.Set("warehouse", warehouse)
	query.Set("staging-dir", (&url.URL{Scheme: "file", Path: stagingDir}).String())
	icebergURI.RawQuery = query.Encode()

	cleanupRemovedChangefeedSinkArtifacts(id, icebergURI.String(), upstreamID)

	recreated := icebergcfg.NewTargetOwnerClaim("other-cdc", 2002, "default/iceberg-recreated", identifier)
	require.NoError(t, icebergcfg.ClaimTargetOwner(ctx, warehouse, recreated))
}

func coordinatorTestPathSegment(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
