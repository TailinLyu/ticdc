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
	uri, err := url.Parse("iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse&staging-dir=file:///tmp/ticdc-stage&database-prefix=tidb_&table-suffix=_log&commit-interval=2s&batch-rows=64&catalog-host-header=catalog.internal&suppress-headers=X-Iceberg-Access-Delegation, X-Test&aws-region=us-west-2&aws-user-agent=ticdc-iceberg-test&table-properties=create_iceberg_table_location_bucket=my-bucket,write.parquet.compression-codec=zstd")
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
	require.Equal(t, "catalog.internal", cfg.CatalogHostHeader)
	require.Equal(t, []string{"X-Iceberg-Access-Delegation", "X-Test"}, cfg.SuppressHeaders)
	require.Equal(t, "us-west-2", cfg.AWSRegion)
	require.Equal(t, "ticdc-iceberg-test", cfg.AWSUserAgent)
	require.Equal(t, "us-west-2", cfg.AWS.Region)
	require.Equal(t, []string{"ticdc-iceberg-test"}, cfg.AWS.UserAgentTags)
	require.Equal(t, map[string]string{
		"create_iceberg_table_location_bucket": "my-bucket",
		"write.parquet.compression-codec":      "zstd",
	}, cfg.TableProperties)
}

func TestParseConfigAcceptsJSONTableProperties(t *testing.T) {
	rawProperties := `{"create_iceberg_table_location_bucket":"my-bucket","write.metadata.compression-codec":"gzip"}`
	uri, err := url.Parse("iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse&table-properties=" + url.QueryEscape(rawProperties))
	require.NoError(t, err)

	cfg, err := ParseConfig(uri)
	require.NoError(t, err)

	require.Equal(t, map[string]string{
		"create_iceberg_table_location_bucket": "my-bucket",
		"write.metadata.compression-codec":     "gzip",
	}, cfg.TableProperties)
}

func TestParseConfigAcceptsStructuredAWSOptions(t *testing.T) {
	rawAWS := `{"region":"us-east-1","endpoint":"https://s3.example.com","path-style-access":false,"user-agent-tags":["iceberg-ready","my-app/1.2"]}`
	uri, err := url.Parse("iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse&aws=" + url.QueryEscape(rawAWS))
	require.NoError(t, err)

	cfg, err := ParseConfig(uri)
	require.NoError(t, err)

	require.Equal(t, "us-east-1", cfg.AWS.Region)
	require.Equal(t, "https://s3.example.com", cfg.AWS.Endpoint)
	require.NotNil(t, cfg.AWS.PathStyleAccess)
	require.False(t, *cfg.AWS.PathStyleAccess)
	require.Equal(t, []string{"iceberg-ready", "my-app/1.2"}, cfg.AWS.UserAgentTags)
	require.Equal(t, "us-east-1", cfg.AWSRegion)
	require.Equal(t, "iceberg-ready", cfg.AWSUserAgent)
}

func TestParseConfigKeepsLegacyAWSAliases(t *testing.T) {
	uri, err := url.Parse("iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse&aws-region=us-west-2&aws-user-agent=ticdc-iceberg-test")
	require.NoError(t, err)

	cfg, err := ParseConfig(uri)
	require.NoError(t, err)

	require.Equal(t, "us-west-2", cfg.AWS.Region)
	require.Equal(t, []string{"ticdc-iceberg-test"}, cfg.AWS.UserAgentTags)
	require.Equal(t, "us-west-2", cfg.AWSRegion)
	require.Equal(t, "ticdc-iceberg-test", cfg.AWSUserAgent)
}

func TestParseConfigRejectsInvalidAWSOptions(t *testing.T) {
	for _, raw := range []string{`{"region":`, `{"user-agent-tags":[""]}`} {
		uri, err := url.Parse("iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse&aws=" + url.QueryEscape(raw))
		require.NoError(t, err)

		_, err = ParseConfig(uri)
		require.Error(t, err)
		require.Contains(t, err.Error(), "aws")
	}
}

func TestParseConfigRejectsInvalidTableProperties(t *testing.T) {
	for _, raw := range []string{"missing-value", `{"bad":`} {
		uri, err := url.Parse("iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse&table-properties=" + url.QueryEscape(raw))
		require.NoError(t, err)

		_, err = ParseConfig(uri)
		require.Error(t, err)
		require.Contains(t, err.Error(), "table-properties")
	}
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
	require.Contains(t, err.Error(), "POSIX filesystem")
	require.Contains(t, err.Error(), "mmap/flock")
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
