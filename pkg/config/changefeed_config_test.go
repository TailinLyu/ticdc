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

package config

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestToChangefeedConfigCarriesUpstreamID(t *testing.T) {
	info := &ChangeFeedInfo{
		ChangefeedID: common.NewChangeFeedIDWithName("iceberg-test", common.DefaultKeyspaceName),
		UpstreamID:   1024,
		Config:       GetDefaultReplicaConfig(),
	}

	cfg := info.ToChangefeedConfig()

	require.Equal(t, uint64(1024), cfg.UpstreamID)
}
