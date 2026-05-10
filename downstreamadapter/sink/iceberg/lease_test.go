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
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func TestClaimTargetOwnerPublishesEtcdCommitterLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	clientURL, embedded, err := etcd.SetupEmbedEtcd(t.TempDir())
	require.NoError(t, err)
	defer embedded.Close()

	rawClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer rawClient.Close()

	etcdClient, err := etcd.NewCDCEtcdClient(ctx, rawClient, "cdc-a")
	require.NoError(t, err)
	lease, err := etcdClient.GetEtcdClient().Grant(ctx, 5)
	require.NoError(t, err)
	session, err := etcdClient.GetEtcdClient().NewSession(concurrency.WithLease(lease.ID))
	require.NoError(t, err)
	defer session.Close()

	appcontext.SetService(appcontext.EtcdClient, etcdClient)
	appcontext.SetService(appcontext.EtcdSession, session)
	t.Cleanup(func() {
		appcontext.DeleteService(appcontext.EtcdClient)
		appcontext.DeleteService(appcontext.EtcdSession)
	})

	cfg := newSinkTestConfig(t, 1024)
	cfg.TiCDCClusterID = "cdc-a"
	cfg.UpstreamID = 1001
	changefeedID := common.NewChangefeedID4Test("default", "iceberg-lease")
	s := newSink(ctx, changefeedID, cfg, &recordingAppendWriter{})
	identifier := []string{"test", "orders_cdc"}

	require.NoError(t, s.claimTargetOwner(ctx, identifier))

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
