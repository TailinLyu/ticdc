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
	IcebergStagedOldestAgeGauge.WithLabelValues("default", "iceberg-test", "pending").Set(3)
	IcebergStagedBytesGauge.WithLabelValues("default", "iceberg-test", "pending").Set(4)
	IcebergCommitDurationHistogram.WithLabelValues("default", "iceberg-test", "success").Observe(0.01)
	IcebergCommittedBatchesCounter.WithLabelValues("default", "iceberg-test").Add(1)
	IcebergCommittedRowsCounter.WithLabelValues("default", "iceberg-test").Add(2)
	IcebergCommitBarrierLagGauge.WithLabelValues("default", "iceberg-test").Set(4)
	IcebergNegativeCommitBarrierLagCounter.WithLabelValues("default", "iceberg-test").Inc()
	IcebergCommittedLedgerEntriesGauge.WithLabelValues("default", "iceberg-test").Set(5)
	IcebergCommittedLedgerWritesCounter.WithLabelValues("default", "iceberg-test", "success").Inc()
	IcebergCommittedLedgerLookupsCounter.WithLabelValues("default", "iceberg-test", "success").Inc()
	IcebergCommittedRowLedgerEntriesGauge.WithLabelValues("default", "iceberg-test").Set(6)
	IcebergCommittedRowLedgerWritesCounter.WithLabelValues("default", "iceberg-test", "success").Inc()
	IcebergCommittedRowLedgerLookupsCounter.WithLabelValues("default", "iceberg-test", "success").Inc()
	IcebergAppendFailureCounter.WithLabelValues("default", "iceberg-test", "append").Inc()
	IcebergCleanupFailureCounter.WithLabelValues("default", "iceberg-test", "staged_file_delete").Inc()
	IcebergStagingBackendInfoGauge.WithLabelValues("default", "iceberg-test", "local_json").Set(1)
	IcebergTargetOwnerConflictCounter.WithLabelValues("default", "iceberg-test").Inc()

	families, err := registry.Gather()
	require.NoError(t, err)

	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	require.Contains(t, names, "ticdc_sink_iceberg_staged_files")
	require.Contains(t, names, "ticdc_sink_iceberg_staged_rows")
	require.Contains(t, names, "ticdc_sink_iceberg_staged_oldest_age_seconds")
	require.Contains(t, names, "ticdc_sink_iceberg_staged_bytes")
	require.Contains(t, names, "ticdc_sink_iceberg_commit_duration_seconds")
	require.Contains(t, names, "ticdc_sink_iceberg_committed_batches_total")
	require.Contains(t, names, "ticdc_sink_iceberg_committed_rows_total")
	require.Contains(t, names, "ticdc_sink_iceberg_commit_barrier_lag_tso")
	require.Contains(t, names, "ticdc_sink_iceberg_negative_commit_barrier_lag_total")
	require.Contains(t, names, "ticdc_sink_iceberg_committed_ledger_entries")
	require.Contains(t, names, "ticdc_sink_iceberg_committed_ledger_writes_total")
	require.Contains(t, names, "ticdc_sink_iceberg_committed_ledger_lookups_total")
	require.Contains(t, names, "ticdc_sink_iceberg_committed_row_ledger_entries")
	require.Contains(t, names, "ticdc_sink_iceberg_committed_row_ledger_writes_total")
	require.Contains(t, names, "ticdc_sink_iceberg_committed_row_ledger_lookups_total")
	require.Contains(t, names, "ticdc_sink_iceberg_append_failures_total")
	require.Contains(t, names, "ticdc_sink_iceberg_cleanup_failures_total")
	require.Contains(t, names, "ticdc_sink_iceberg_staging_backend_info")
	require.Contains(t, names, "ticdc_sink_iceberg_target_owner_conflicts_total")
}
