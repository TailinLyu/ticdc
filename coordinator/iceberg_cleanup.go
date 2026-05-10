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
	"net/url"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	icebergcfg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"go.uber.org/zap"
)

func cleanupRemovedChangefeedSinkArtifacts(id common.ChangeFeedID, sinkURI string, upstreamID uint64) {
	parsedURI, err := url.Parse(sinkURI)
	if err != nil {
		log.Warn("skip removed changefeed sink artifact cleanup, sink uri is invalid",
			zap.Stringer("changefeed", id),
			zap.Error(err))
		return
	}
	if !strings.EqualFold(parsedURI.Scheme, "iceberg") {
		return
	}

	cfg, err := icebergcfg.ParseConfig(parsedURI)
	if err != nil {
		log.Warn("skip removed iceberg changefeed staging cleanup, sink uri config is invalid",
			zap.Stringer("changefeed", id),
			zap.Error(err))
		return
	}
	if err := icebergcfg.CleanupChangefeedStaging(cfg.StagingDir, id.String()); err != nil {
		log.Warn("failed to clean removed iceberg changefeed staging",
			zap.Stringer("changefeed", id),
			zap.String("stagingDir", cfg.StagingDir),
			zap.Error(err))
	}
	ownerID := icebergcfg.TargetOwnerID(config.GetGlobalServerConfig().ClusterID, upstreamID, id.String())
	if err := icebergcfg.CleanupTargetOwnerClaims(context.Background(), cfg.Warehouse, ownerID); err != nil {
		log.Warn("failed to clean removed iceberg warehouse target owner claim",
			zap.Stringer("changefeed", id),
			zap.String("ownerID", ownerID),
			zap.String("warehouse", cfg.Warehouse),
			zap.Error(err))
		return
	}
	log.Info("cleaned removed iceberg changefeed artifacts",
		zap.Stringer("changefeed", id),
		zap.String("stagingDir", cfg.StagingDir),
		zap.String("warehouse", cfg.Warehouse),
		zap.String("ownerID", ownerID))
}
