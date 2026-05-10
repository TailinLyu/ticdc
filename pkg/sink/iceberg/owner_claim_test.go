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
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestClaimTargetOwnerRejectsDifferentOwnerInSharedWarehouse(t *testing.T) {
	ctx := context.Background()
	warehouse := (&url.URL{Scheme: "file", Path: t.TempDir()}).String()
	identifier := []string{"db", "orders_cdc"}
	left := NewTargetOwnerClaim("cdc-a", 1001, "default/left", identifier)
	right := NewTargetOwnerClaim("cdc-b", 2002, "default/right", identifier)

	require.NoError(t, ClaimTargetOwner(ctx, warehouse, left))
	require.NoError(t, ClaimTargetOwner(ctx, warehouse, left))

	err := ClaimTargetOwner(ctx, warehouse, right)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTargetOwnerConflict))
}

func TestCleanupTargetOwnerClaimsRemovesOnlyMatchingOwner(t *testing.T) {
	ctx := context.Background()
	warehouse := (&url.URL{Scheme: "file", Path: t.TempDir()}).String()
	leftOrders := NewTargetOwnerClaim("cdc-a", 1001, "default/left", []string{"db", "orders_cdc"})
	leftCustomers := NewTargetOwnerClaim("cdc-a", 1001, "default/left", []string{"db", "customers_cdc"})
	rightOrders := NewTargetOwnerClaim("cdc-b", 2002, "default/right", []string{"db", "orders_cdc"})

	require.NoError(t, ClaimTargetOwner(ctx, warehouse, leftOrders))
	require.NoError(t, ClaimTargetOwner(ctx, warehouse, leftCustomers))
	require.NoError(t, CleanupTargetOwnerClaims(ctx, warehouse, leftOrders.OwnerID))

	require.NoError(t, ClaimTargetOwner(ctx, warehouse, rightOrders))
	err := ClaimTargetOwner(ctx, warehouse, leftOrders)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTargetOwnerConflict))

	require.NoError(t, ClaimTargetOwner(ctx, warehouse, leftCustomers))
}

func TestClaimTargetOwnerAllowsOnlyOneConcurrentOwner(t *testing.T) {
	ctx := context.Background()
	warehouse := (&url.URL{Scheme: "file", Path: t.TempDir()}).String()

	assertExactlyOneConcurrentOwner(t, ctx, warehouse, []string{"db", "orders_concurrent_cdc"})
}

func TestTargetOwnerIDIncludesUpstreamCluster(t *testing.T) {
	require.NotEqual(t,
		TargetOwnerID("cdc", 1001, "default/orders"),
		TargetOwnerID("cdc", 2002, "default/orders"))
	require.NotEqual(t,
		TargetOwnerID("cdc-a", 1001, "default/orders"),
		TargetOwnerID("cdc-b", 1001, "default/orders"))
}

func TestReadTargetOwnerClaimIfExistsSkipsMissingRead(t *testing.T) {
	store := &targetOwnerReadStore{exists: false}

	_, ok, err := readTargetOwnerClaimIfExists(context.Background(), store, "missing.lock")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 0, store.readCalls)
}

func TestClaimTargetOwnerAgainstMinIO(t *testing.T) {
	warehouse := os.Getenv("ICEBERG_MINIO_WAREHOUSE")
	if warehouse == "" {
		t.Skip("set ICEBERG_MINIO_WAREHOUSE to run MinIO owner-marker coverage")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	identifier := []string{"minio_db", "orders_cdc"}
	left := NewTargetOwnerClaim("cdc-minio-left", 1001, "default/minio-left", identifier)
	right := NewTargetOwnerClaim("cdc-minio-right", 2002, "default/minio-right", identifier)

	require.NoError(t, CleanupTargetOwnerClaims(ctx, warehouse, left.OwnerID))
	require.NoError(t, CleanupTargetOwnerClaims(ctx, warehouse, right.OwnerID))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		require.NoError(t, CleanupTargetOwnerClaims(cleanupCtx, warehouse, left.OwnerID))
		require.NoError(t, CleanupTargetOwnerClaims(cleanupCtx, warehouse, right.OwnerID))
	})

	require.NoError(t, ClaimTargetOwner(ctx, warehouse, left))
	require.NoError(t, ClaimTargetOwner(ctx, warehouse, left))

	err := ClaimTargetOwner(ctx, warehouse, right)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTargetOwnerConflict))

	require.NoError(t, CleanupTargetOwnerClaims(ctx, warehouse, left.OwnerID))
	require.NoError(t, ClaimTargetOwner(ctx, warehouse, right))

	assertExactlyOneConcurrentOwner(t, ctx, warehouse, []string{"minio_db", "orders_concurrent_cdc"})
}

func assertExactlyOneConcurrentOwner(
	t *testing.T,
	ctx context.Context,
	warehouse string,
	identifier []string,
) {
	t.Helper()

	const owners = 8
	start := make(chan struct{})
	results := make(chan struct {
		claim TargetOwnerClaim
		err   error
	}, owners)

	var wg sync.WaitGroup
	for i := 0; i < owners; i++ {
		claim := NewTargetOwnerClaim(
			fmt.Sprintf("cdc-concurrent-%02d", i),
			uint64(1000+i),
			fmt.Sprintf("default/concurrent-%02d", i),
			identifier)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- struct {
				claim TargetOwnerClaim
				err   error
			}{claim: claim, err: ClaimTargetOwner(ctx, warehouse, claim)}
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	var winner TargetOwnerClaim
	successes := 0
	conflicts := 0
	for result := range results {
		if result.err == nil {
			successes++
			winner = result.claim
			continue
		}
		if errors.Is(result.err, ErrTargetOwnerConflict) {
			conflicts++
			continue
		}
		require.NoError(t, result.err)
	}

	require.Equal(t, 1, successes)
	require.Equal(t, owners-1, conflicts)
	require.NoError(t, CleanupTargetOwnerClaims(ctx, warehouse, winner.OwnerID))
}

type targetOwnerReadStore struct {
	storage.ExternalStorage
	exists    bool
	readCalls int
}

func (s *targetOwnerReadStore) FileExists(context.Context, string) (bool, error) {
	return s.exists, nil
}

func (s *targetOwnerReadStore) ReadFile(context.Context, string) ([]byte, error) {
	s.readCalls++
	return nil, os.ErrNotExist
}
