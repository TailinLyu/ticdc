// Copyright 2025 PingCAP, Inc.
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

package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newRemoteMessageTargetForTest() *remoteMessageTarget {
	localId := node.NewID()
	remoteId := node.NewID()
	ctx := context.Background()
	cfg := config.NewDefaultMessageCenterConfig("")
	receivedMsgCh := make(chan *TargetMessage, 1)
	rt := newRemoteMessageTarget(ctx, localId, remoteId, "", "", receivedMsgCh, receivedMsgCh, cfg, nil)
	return rt
}

func TestRemoteTargetNewMessage(t *testing.T) {
	rt := newRemoteMessageTargetForTest()
	defer rt.close()

	msg := &TargetMessage{
		Type: TypeMessageHandShake,
	}
	msg1 := rt.newMessage(msg)
	require.Equal(t, TypeMessageHandShake, IOType(msg1.Type))

	msg2 := rt.newMessage(msg)
	log.Info("msg2", zap.Any("msg2", msg2))
	require.Equal(t, TypeMessageHandShake, IOType(msg2.Type))
}

func TestRemoteTargetReconnectBackoffSuppressesImmediateRetry(t *testing.T) {
	rt := &remoteMessageTarget{}
	now := time.Unix(10, 0)

	oldBackoff := remoteTargetReconnectBackoff
	defer func() {
		remoteTargetReconnectBackoff = oldBackoff
	}()
	remoteTargetReconnectBackoff = func(attempt int) time.Duration {
		return time.Duration(attempt) * time.Second
	}

	require.True(t, rt.canResetConnection(now))
	rt.noteReconnectFailure(now)
	require.False(t, rt.canResetConnection(now.Add(500*time.Millisecond)))
	require.True(t, rt.canResetConnection(now.Add(time.Second)))

	rt.noteReconnectFailure(now.Add(time.Second))
	require.False(t, rt.canResetConnection(now.Add(2*time.Second)))
	require.True(t, rt.canResetConnection(now.Add(3*time.Second)))

	rt.noteReconnectSuccess()
	require.True(t, rt.canResetConnection(now.Add(1500*time.Millisecond)))
}
