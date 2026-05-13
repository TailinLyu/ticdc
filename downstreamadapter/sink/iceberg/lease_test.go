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
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func TestClaimTargetOwnerPublishesEtcdCommitterLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	etcdClient := setupEtcdCommitterLease(t, ctx, "cdc-a")
	cfg := newSinkTestConfig(t, 1024)
	cfg.TiCDCClusterID = "cdc-a"
	cfg.UpstreamID = 1001
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-lease")
	s := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})
	identifier := []string{"test", "orders_cdc"}

	ownsLease, err := s.claimTargetOwnerForDrain(ctx, identifier)
	require.NoError(t, err)
	require.True(t, ownsLease)

	prefix := etcd.IcebergCommitterElectionKey("cdc-a", identifierKey(identifier))
	resp, err := etcdClient.GetEtcdClient().Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Contains(t, string(resp.Kvs[0].Value), s.ownerID)
	require.Contains(t, string(resp.Kvs[0].Value), "orders_cdc")

	s.Close(false)
	resp, err = etcdClient.GetEtcdClient().Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, resp.Kvs)
}

func TestClaimTargetOwnerAllowsSameEtcdCommitterLeaseOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	etcdClient := setupEtcdCommitterLease(t, ctx, "cdc-a")
	cfg := newSinkTestConfig(t, 1024)
	cfg.TiCDCClusterID = "cdc-a"
	cfg.UpstreamID = 1001
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-lease")
	identifier := []string{"test", "orders_cdc"}
	first := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})
	second := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})

	ownsLease, err := first.claimTargetOwnerForDrain(ctx, identifier)
	require.NoError(t, err)
	require.True(t, ownsLease)

	start := time.Now()
	ownsLease, err = second.claimTargetOwnerForDrain(ctx, identifier)
	require.NoError(t, err)
	require.False(t, ownsLease)
	require.Less(t, time.Since(start), time.Second)

	prefix := etcd.IcebergCommitterElectionKey("cdc-a", identifierKey(identifier))
	resp, err := etcdClient.GetEtcdClient().Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Contains(t, string(resp.Kvs[0].Value), first.ownerID)

	second.Close(false)
	resp, err = etcdClient.GetEtcdClient().Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)

	first.Close(false)
	resp, err = etcdClient.GetEtcdClient().Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, resp.Kvs)
}

func TestClaimTargetOwnerRejectsDifferentEtcdCommitterLeaseOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	setupEtcdCommitterLease(t, ctx, "cdc-a")
	cfg := newSinkTestConfig(t, 1024)
	cfg.TiCDCClusterID = "cdc-a"
	cfg.UpstreamID = 1001
	identifier := []string{"test", "orders_cdc"}
	left := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-left"), cfg, &recordingAppendWriter{})
	right := newSink(ctx, common.NewChangefeedID4Test("default", "iceberg-right"), cfg, &recordingAppendWriter{})
	defer left.Close(false)
	defer right.Close(false)

	ownsLease, err := left.claimTargetCommitterLease(ctx, identifier)
	require.NoError(t, err)
	require.True(t, ownsLease)

	ownsLease, err = right.claimTargetCommitterLease(ctx, identifier)
	require.Error(t, err)
	require.False(t, ownsLease)
	require.True(t, cerror.Is(err, errIcebergTargetOwnerConflict))
}

func setupEtcdCommitterLease(
	t *testing.T,
	ctx context.Context,
	clusterID string,
) *etcd.CDCEtcdClientImpl {
	t.Helper()

	clientURL, embedded, err := etcd.SetupEmbedEtcd(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(embedded.Close)

	rawClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, rawClient.Close())
	})

	etcdClient, err := etcd.NewCDCEtcdClient(ctx, rawClient, clusterID)
	require.NoError(t, err)
	lease, err := etcdClient.GetEtcdClient().Grant(ctx, 5)
	require.NoError(t, err)
	session, err := etcdClient.GetEtcdClient().NewSession(concurrency.WithLease(lease.ID))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, session.Close())
		appcontext.DeleteService(appcontext.EtcdClient)
		appcontext.DeleteService(appcontext.EtcdSession)
	})

	appcontext.SetService(appcontext.EtcdClient, etcdClient)
	appcontext.SetService(appcontext.EtcdSession, session)
	return etcdClient
}
