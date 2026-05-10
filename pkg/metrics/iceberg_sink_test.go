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

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestInitSinkMetricsRegistersIcebergMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	initSinkMetrics(registry)
	IcebergStagedFilesGauge.WithLabelValues("default", "iceberg-test", "pending").Set(1)
	IcebergStagedRowsGauge.WithLabelValues("default", "iceberg-test", "pending").Set(2)
	IcebergTargetOwnerConflictCounter.WithLabelValues("default", "iceberg-test").Inc()

	families, err := registry.Gather()
	require.NoError(t, err)

	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	require.Contains(t, names, "ticdc_sink_iceberg_staged_files")
	require.Contains(t, names, "ticdc_sink_iceberg_staged_rows")
	require.Contains(t, names, "ticdc_sink_iceberg_target_owner_conflicts_total")
}
