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
	"encoding/json"
	stderrors "errors"
	"net/http"
	"net/http/httptest"
	"testing"

	iceberggo "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	icebergutils "github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/stretchr/testify/require"
)

type namespaceProbeCatalog struct {
	createErr error
	exists    bool
	checkErr  error
}

func (c namespaceProbeCatalog) CreateNamespace(
	context.Context,
	icebergtable.Identifier,
	iceberggo.Properties,
) error {
	return c.createErr
}

func (c namespaceProbeCatalog) CheckNamespaceExists(
	context.Context,
	icebergtable.Identifier,
) (bool, error) {
	return c.exists, c.checkErr
}

func TestEnsureNamespaceContinuesWhenCreateErrorLeavesNamespaceExisting(t *testing.T) {
	err := ensureNamespace(context.Background(), namespaceProbeCatalog{
		createErr: stderrors.New("sqlite catalog busy"),
		exists:    true,
	}, icebergtable.Identifier{"test"})

	require.NoError(t, err)
}

func TestEnsureNamespaceReturnsCreateErrorWhenNamespaceStillMissing(t *testing.T) {
	err := ensureNamespace(context.Background(), namespaceProbeCatalog{
		createErr: stderrors.New("sqlite catalog busy"),
		exists:    false,
	}, icebergtable.Identifier{"test"})

	require.Error(t, err)
	require.Contains(t, err.Error(), "sqlite catalog busy")
}

func TestSnapshotCommittedBatchesForCandidatesFiltersUnrequestedHistory(t *testing.T) {
	snapshots := []icebergtable.Snapshot{
		{Summary: &icebergtable.Summary{Properties: iceberggo.Properties{
			snapshotBatchIDKey: "old-unrequested",
		}}},
		{Summary: &icebergtable.Summary{Properties: iceberggo.Properties{
			snapshotBatchIDsKey: "batch-a,batch-b",
		}}},
		{Summary: &icebergtable.Summary{Properties: iceberggo.Properties{
			snapshotBatchIDKey: "latest",
		}}},
	}

	committed := snapshotCommittedBatchesForCandidates(snapshots, []string{"batch-a", "latest", "missing"})

	require.Contains(t, committed, "batch-a")
	require.Contains(t, committed, "latest")
	require.NotContains(t, committed, "batch-b")
	require.NotContains(t, committed, "old-unrequested")
	require.NotContains(t, committed, "missing")
}

func TestCanPromoteIcebergTypeRejectsNarrowingDDL(t *testing.T) {
	require.True(t, canPromoteIcebergType(iceberggo.PrimitiveTypes.Int32, iceberggo.PrimitiveTypes.Int64))
	require.True(t, canPromoteIcebergType(iceberggo.DecimalTypeOf(10, 2), iceberggo.DecimalTypeOf(12, 4)))
	require.False(t, canPromoteIcebergType(iceberggo.PrimitiveTypes.Int64, iceberggo.PrimitiveTypes.Int32))
	require.False(t, canPromoteIcebergType(iceberggo.DecimalTypeOf(12, 4), iceberggo.DecimalTypeOf(10, 2)))
	require.False(t, canPromoteIcebergType(iceberggo.PrimitiveTypes.TimestampTz, iceberggo.PrimitiveTypes.Timestamp))
}

func TestNewIcebergAppenderAppliesCatalogTransportOverrides(t *testing.T) {
	var seenHost string
	var seenHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/config" {
			http.NotFound(w, r)
			return
		}
		seenHost = r.Host
		seenHeaders = r.Header.Clone()
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		}))
	}))
	defer server.Close()

	_, err := newIcebergAppender(context.Background(), &icebergcfg.Config{
		CatalogURI:        server.URL,
		Warehouse:         "file:///tmp/warehouse",
		CatalogHostHeader: "catalog.internal",
		SuppressHeaders:   []string{"X-Iceberg-Access-Delegation"},
		AWSRegion:         "us-west-2",
		AWSUserAgent:      "ticdc-iceberg-test",
	})
	require.NoError(t, err)

	require.Equal(t, "catalog.internal", seenHost)
	require.Empty(t, seenHeaders.Values("X-Iceberg-Access-Delegation"))
}

func TestRestCatalogOptionsPlumbsAWSConfigIntoIcebergIOContext(t *testing.T) {
	ctx, _, err := restCatalogOptions(context.Background(), &icebergcfg.Config{
		Warehouse:    "s3://bucket/warehouse",
		AWSRegion:    "us-west-2",
		AWSUserAgent: "ticdc-iceberg-test",
	})
	require.NoError(t, err)

	awsCfg := icebergutils.GetAwsConfig(ctx)
	require.NotNil(t, awsCfg)
	require.Equal(t, "us-west-2", awsCfg.Region)
	require.NotEmpty(t, awsCfg.APIOptions)
}

func TestIcebergAppenderPlumbsAWSConfigIntoRuntimeContext(t *testing.T) {
	appender := &icebergAppender{
		awsCfg: &aws.Config{Region: "us-west-2"},
	}

	ctx := appender.ctxWithAWS(context.Background())
	awsCfg := icebergutils.GetAwsConfig(ctx)
	require.NotNil(t, awsCfg)
	require.Equal(t, "us-west-2", awsCfg.Region)
}

type loadTableProbeCatalog struct {
	catalog.Catalog

	loadCtx context.Context
	loadErr error
}

func (c *loadTableProbeCatalog) LoadTable(
	ctx context.Context,
	_ icebergtable.Identifier,
) (*icebergtable.Table, error) {
	c.loadCtx = ctx
	return nil, c.loadErr
}

func TestAppendRowsPlumbsAWSConfigIntoIcebergLoadContext(t *testing.T) {
	loadErr := stderrors.New("stop after load")
	cat := &loadTableProbeCatalog{loadErr: loadErr}
	appender := &icebergAppender{
		catalog: cat,
		awsCfg:  &aws.Config{Region: "us-west-2"},
	}

	err := appender.AppendRows(context.Background(),
		[]string{"db", "tbl"},
		[]map[string]any{{"id": int64(1)}},
		nil,
		iceberggo.Properties{})

	require.ErrorContains(t, err, loadErr.Error())
	awsCfg := icebergutils.GetAwsConfig(cat.loadCtx)
	require.NotNil(t, awsCfg)
	require.Equal(t, "us-west-2", awsCfg.Region)
}

func TestNewIcebergAppenderKeepsAWSConfigForRuntimeContexts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/config" {
			http.NotFound(w, r)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		}))
	}))
	defer server.Close()

	appender, err := newIcebergAppender(context.Background(), &icebergcfg.Config{
		CatalogURI:   server.URL,
		Warehouse:    "file:///tmp/warehouse",
		AWSRegion:    "us-west-2",
		AWSUserAgent: "ticdc-iceberg-test",
	})
	require.NoError(t, err)

	awsCfg := icebergutils.GetAwsConfig(appender.ctxWithAWS(context.Background()))
	require.NotNil(t, awsCfg)
	require.Equal(t, "us-west-2", awsCfg.Region)
	require.NotEmpty(t, awsCfg.APIOptions)
}

func TestIcebergAppenderLeavesRuntimeContextUnchangedWithoutAWSConfig(t *testing.T) {
	appender := &icebergAppender{}
	ctx := context.Background()

	require.Equal(t, ctx, appender.ctxWithAWS(ctx))
}

func TestNewIcebergAppenderAuthModeNoneDropsAuthorization(t *testing.T) {
	var seenHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/config" {
			http.NotFound(w, r)
			return
		}
		seenHeaders = r.Header.Clone()
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		}))
	}))
	defer server.Close()

	_, err := newIcebergAppender(context.Background(), &icebergcfg.Config{
		CatalogURI:      server.URL,
		Warehouse:       "file:///tmp/warehouse",
		CatalogAuthMode: "none",
	})
	require.NoError(t, err)

	require.Empty(t, seenHeaders.Values("Authorization"))
}

func TestIcebergCloudIOSchemesAreRegistered(t *testing.T) {
	require.Contains(t, icebergio.GetRegisteredSchemes(), "s3")
}

func TestCreateTablePropertiesMergesOperatorPropertiesWithoutOverridingManagedKeys(t *testing.T) {
	props := createTableProperties(map[string]string{
		"create_iceberg_table_location_bucket": "my-bucket",
		"write.parquet.compression-codec":      "zstd",
		"format-version":                       "1",
		tableOwnerIDKey:                        "other-owner",
	}, "owner-a", "default/cf", "cluster-a", "1001")

	require.Equal(t, "my-bucket", props["create_iceberg_table_location_bucket"])
	require.Equal(t, "zstd", props["write.parquet.compression-codec"])
	require.Equal(t, "2", props["format-version"])
	require.Equal(t, "parquet", props["write.format.default"])
	require.Equal(t, "owner-a", props[tableOwnerIDKey])
	require.Equal(t, "default/cf", props[tableOwnerKey])
	require.Equal(t, "cluster-a", props[tableCDCClusterIDKey])
	require.Equal(t, "1001", props[tableUpstreamIDKey])
}
