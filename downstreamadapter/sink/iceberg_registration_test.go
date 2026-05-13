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

package sink

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewIcebergSink(t *testing.T) {
	replicaCfg := config.GetDefaultReplicaConfig()
	cfg := &config.ChangefeedConfig{
		SinkURI:    "iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse",
		SinkConfig: replicaCfg.Sink,
	}

	s, err := New(context.Background(), cfg, common.NewChangeFeedIDWithName("iceberg-test", common.DefaultKeyspaceName))
	require.NoError(t, err)
	require.Equal(t, common.IcebergSinkType, s.SinkType())
}
