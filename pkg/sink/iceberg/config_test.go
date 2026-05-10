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
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseConfigFromSinkURI(t *testing.T) {
	uri, err := url.Parse("iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse&staging-dir=file:///tmp/ticdc-stage&database-prefix=tidb_&table-suffix=_log&commit-interval=2s&batch-rows=64")
	require.NoError(t, err)

	cfg, err := ParseConfig(uri)
	require.NoError(t, err)

	require.Equal(t, "http://localhost:8181/", cfg.CatalogURI)
	require.Equal(t, "file:///tmp/iceberg-warehouse", cfg.Warehouse)
	require.Equal(t, "/tmp/ticdc-stage", cfg.StagingDir)
	require.Equal(t, "tidb_", cfg.DatabasePrefix)
	require.Equal(t, "_log", cfg.TableSuffix)
	require.Equal(t, 2*time.Second, cfg.CommitInterval)
	require.Equal(t, 64, cfg.BatchRows)
}

func TestParseConfigAllowsExplicitCatalogURI(t *testing.T) {
	uri, err := url.Parse("iceberg://ignored:8181/?catalog-uri=https://catalog.example.test/iceberg&warehouse=file:///tmp/warehouse")
	require.NoError(t, err)

	cfg, err := ParseConfig(uri)
	require.NoError(t, err)

	require.Equal(t, "https://catalog.example.test/iceberg", cfg.CatalogURI)
	require.Equal(t, "file:///tmp/warehouse", cfg.Warehouse)
	require.Equal(t, "/tmp/warehouse/.ticdc-staging", cfg.StagingDir)
	require.Equal(t, "_cdc", cfg.TableSuffix)
}

func TestParseConfigRequiresExplicitStagingDirForRemoteWarehouse(t *testing.T) {
	uri, err := url.Parse("iceberg://localhost:8181/?warehouse=s3://bucket/warehouse")
	require.NoError(t, err)

	_, err = ParseConfig(uri)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must include staging-dir")
}

func TestParseConfigAllowsRemoteWarehouseWithExplicitStagingDir(t *testing.T) {
	uri, err := url.Parse("iceberg://localhost:8181/?warehouse=s3://bucket/warehouse&staging-dir=file:///mnt/shared/ticdc-stage")
	require.NoError(t, err)

	cfg, err := ParseConfig(uri)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/warehouse", cfg.Warehouse)
	require.Equal(t, "/mnt/shared/ticdc-stage", cfg.StagingDir)
}

func TestParseConfigRejectsUnsupportedRemoteWarehouseScheme(t *testing.T) {
	uri, err := url.Parse("iceberg://localhost:8181/?warehouse=gs://bucket/warehouse&staging-dir=file:///mnt/shared/ticdc-stage")
	require.NoError(t, err)

	_, err = ParseConfig(uri)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported iceberg warehouse scheme")
}

func TestTargetIdentifier(t *testing.T) {
	cfg := Config{
		DatabasePrefix: "tidb_",
		TableSuffix:    "_cdc",
	}

	require.Equal(t, []string{"tidb_app", "users_cdc"}, cfg.TargetIdentifier("app", "users"))
}
