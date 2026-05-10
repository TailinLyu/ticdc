# Local TiCDC Iceberg E2E

This directory contains the local Iceberg sink environment and validation
artifacts used for the TiCDC v8.5.7-release Iceberg investigation.

The goal is to prove, on one local machine, that multiple TiCDC captures can act
as per-span writers while exactly one capture acts as the Iceberg committer for a
changefeed. The tests also cover multi-table and multi-changefeed layouts,
failure recovery, high concurrency, and SQL readback from Iceberg.

## What This Adds

- `docker-compose.yml`: local PD, TiKV, TiDB, three TiCDC captures, Iceberg REST,
  PrestoDB, and Spark.
- `ticdc.Dockerfile`: image wrapper for the locally built TiCDC binary.
- `workload/main.go`: concurrent TiDB workload generator. It creates/splits
  source tables, writes deterministic successful DML, and prints exact expected
  CDC event counts.
- `icebergread/main.go`: Iceberg REST readback helper for row counts by CDC op.
- `tidbexec/main.go`: tiny SQL helper used by the test harness.
- `iceberg_case_lib.sh`: shell helpers for changefeed creation, readback waits,
  staged-file counting, and log-based writer/committer ownership checks.
- `run_s15_owner_guard.sh`: repeatable local scenario that verifies
  many-changefeed-to-one-target is rejected by the warehouse owner marker.
- `ICEBERG_RESILIENCE_MATRIX.md`: durable scenario checklist.
- `ICEBERG_RESILIENCE_RESULTS.md`: scenario-by-scenario evidence and final
  verification output.
- `presto/etc/catalog/iceberg.properties`: Presto Iceberg REST catalog config.
- `spark/conf/spark-defaults.conf`: Spark Iceberg REST catalog config.

The Iceberg sink implementation lives under:

- `downstreamadapter/sink/iceberg/`
- `pkg/sink/iceberg/`

It is registered through the normal sink factory with the `iceberg://` URI
scheme.

The original implementation design doc from PR #1 is included at
`docs/design/2026-05-07-ticdc-iceberg-sink-design-generic.md`.

## Local Stack

The local stack intentionally uses:

- TiCDC: local branch/image based on v8.5.7-release.
- TiDB, PD, TiKV: v8.5.6 container images.
- Iceberg REST: `tabulario/iceberg-rest:latest`.
- PrestoDB: `prestodb/presto:0.297`.
- Spark: `tabulario/spark-iceberg:latest`.
- Querybook: cloned separately at `/Users/tailinlyu/code/querybook` and connected
  to Presto through `presto://presto:8080/iceberg`.

The warehouse is mounted at `/tmp/iceberg-warehouse`. The TiCDC staging path is
`/tmp/iceberg-warehouse/.ticdc-staging`.

## Build And Start

From the TiCDC repo root:

```bash
# Build a failpoint-enabled TiCDC binary for the local Docker image.
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 make build-cdc-with-failpoint
cp bin/cdc bin/cdc-linux-arm64

# Build the local TiCDC image.
docker build -f local-e2e/ticdc.Dockerfile -t local/ticdc-iceberg:e2e .

# Start the core stack.
rm -rf /tmp/iceberg-warehouse
mkdir -p /tmp/iceberg-warehouse
docker compose -f local-e2e/docker-compose.yml up -d \
  iceberg-rest pd tikv tidb ticdc-1 ticdc-2 ticdc-3

# Optional query engines.
docker compose -f local-e2e/docker-compose.yml up -d presto spark
```

The three TiCDC APIs are mapped to host ports `8300`, `8301`, and `8302`.
Inside each TiCDC container, the local API is still `127.0.0.1:8300`.

## Changefeed Shape

The helper creates changefeeds with table-across-nodes enabled:

```toml
[scheduler]
enable-table-across-nodes = true
region-threshold = 1
region-count-per-span = 1
force-split = true
```

The Iceberg sink URI used by the harness is:

```text
iceberg://iceberg-rest:8181/?warehouse=file:///tmp/iceberg-warehouse&commit-interval=1s&batch-rows=25&staging-dir=file:///tmp/iceberg-warehouse/.ticdc-staging&table-suffix=_cdc
```

Source tables are split into multiple TiKV regions so the new scheduler can
assign multiple spans across local TiCDC captures.

## Querying Iceberg

Go helper:

```bash
go run ./local-e2e/icebergread \
  --summary \
  --warehouse file:///tmp/iceberg-warehouse \
  --catalog http://127.0.0.1:8181 \
  --namespace ice_s21 \
  --table orders_cdc
```

Presto:

```bash
docker compose -f local-e2e/docker-compose.yml exec -T presto \
  /opt/presto-cli --server localhost:8080 --catalog iceberg --schema ice_s21 \
  --execute "SELECT _op, count(*) FROM orders_cdc GROUP BY _op ORDER BY _op"
```

Spark:

```bash
docker compose -f local-e2e/docker-compose.yml exec -T spark \
  spark-sql -e \
  "SELECT _op, count(*) FROM rest.ice_s21.orders_cdc GROUP BY _op ORDER BY _op"
```

Querybook:

1. Start Querybook from `/Users/tailinlyu/code/querybook`.
2. Connect `querybook_web`, `querybook_worker`, and `querybook_scheduler` to the
   `ticdc-iceberg-e2e_default` Docker network.
3. Use environment `local_iceberg` and engine `local_presto_iceberg`.
4. Open `http://127.0.0.1:10001/local_iceberg/`.

Verified Querybook query:

```sql
SELECT _op, count(*) AS c
FROM ice_s21.orders_cdc
GROUP BY _op
ORDER BY _op;
```

Expected result from the final run:

```text
D 400
I 6000
U 600
```

## Test Approach

The matrix covers:

- Single changefeed, one table, multiple spans.
- Single changefeed, multiple source tables.
- Multiple changefeeds to different target tables.
- Unsafe multiple changefeeds to the same target table.
- 1:1, 1:many, many:1, and many:many source-to-target layouts.
- Active committer death for `ticdc-1`, `ticdc-2`, and `ticdc-3`.
- Non-committer writer death while the committer survives.
- All-capture outage and restart.
- Iceberg REST outage.
- Staging path failure.
- Staged-file delete failure.
- DDL behavior.
- Split, merge, and move scheduling while writing.
- High concurrency with hundreds of emitted rows per millisecond.

The deterministic crash-window tests use local failpoints and in-package hooks
around these windows:

- after staging but before `PostFlush`
- after Iceberg append but before staged-file delete
- before committer staged-file drain

These hooks make replay/idempotency bugs reproducible instead of relying on
timing luck.

## Current Limitations Found

The historical matrix and fresh reruns are documented in
`ICEBERG_RESILIENCE_RESULTS.md` and summarized in `TEST_FAILURES.md`.

Resolved in the latest local loop:

- The deterministic replay windows S03, S05, and S11 now read back exact counts
  and drain staging to zero.
- The rolling restart replay scenario S20 now reads back exact counts.
- Iceberg REST outage recovery S09 now drains automatically after REST returns,
  without a TiCDC restart.
- The high-volume staged drain S12 now passes at `10000/1000/666` local events.

Remaining intentional limitations:

- Multiple changefeeds writing the same Iceberg target table are unsupported.
  The sink now records a target-owner marker under the shared warehouse and
  rejects a second owner before appending, including across TiCDC clusters that
  share an S3/file warehouse. The writer claims the target before exposing a
  staged file, so a doomed same-target feed does not advance source progress or
  leave rows for the committer. This affects S15 and S24.
- MinIO/S3-compatible coverage exists for the owner marker through
  `run_s16_minio_owner_marker.sh`; staged files are still local/shared-path
  JSON and need follow-up before S3-native high-throughput staging. For now,
  every TiCDC capture must see the same `staging-dir` path, typically through a
  shared filesystem or PVC. A staged JSON batch is made visible only after temp
  write, file fsync, close, atomic rename, and parent directory fsync.
- Replay dedupe now uses both Iceberg snapshot summaries and a durable committed
  ledger under `.committed` in the shared staging directory. Operators must retain
  staged files and committed ledger markers for at least the maximum replay
  horizon. Ledger lookup is direct by candidate marker path, and Iceberg
  snapshot-summary dedupe scans newest-first and stops once the current staged
  batch candidates are found. Partial-overlap replay is restart-safe because
  staged-file cleanup is deferred until the end of a drain, and committed staged
  evidence is retained while the same target still has later staged files
  waiting; duplicate-only replay batches are marked handled before cleanup, and
  the bounded row-ID cache is only an in-process optimization. Native Iceberg
  data-file committables remain the target production design.
- Iceberg schema evolution DDL and live `CREATE TABLE` DDL are unsupported.
  `run_s17_schema_unsupported.sh` and
  `run_s17_create_table_unsupported.sh` verify that live DDL is rejected
  explicitly instead of silently producing partial semantics. Bootstrap/not-sync
  create DDL is still allowed.
- Iceberg tables are created from TiDB `TableInfo`, with `dt` and `hr` identity
  partition fields. Snapshot summaries use `ticdc.commit-barrier-ts`; the older
  `ticdc.checkpoint-ts` name is not emitted.

The highest-priority follow-up is replacing JSON row staging with Iceberg-native
data-file committables and a target-level commit protocol.

## Verification Commands

Fresh verification used before opening the PR:

```bash
go test ./downstreamadapter/sink/iceberg ./pkg/sink/iceberg ./pkg/metrics ./local-e2e \
  ./local-e2e/workload ./local-e2e/icebergread ./local-e2e/tidbexec -count=1
```

Useful Iceberg sink metric families to inspect during a local run:

```bash
ticdc_sink_iceberg_staged_files
ticdc_sink_iceberg_staged_rows
ticdc_sink_iceberg_staged_bytes
ticdc_sink_iceberg_staged_oldest_age_seconds
ticdc_sink_iceberg_commit_duration_seconds
ticdc_sink_iceberg_committed_batches_total
ticdc_sink_iceberg_committed_rows_total
ticdc_sink_iceberg_committed_ledger_entries
ticdc_sink_iceberg_committed_ledger_writes_total
ticdc_sink_iceberg_committed_ledger_lookups_total
ticdc_sink_iceberg_staging_backend_info
ticdc_sink_iceberg_commit_barrier_lag_tso
ticdc_sink_iceberg_append_failures_total
ticdc_sink_iceberg_cleanup_failures_total
ticdc_sink_iceberg_target_owner_conflicts_total
```

Minimum local alert guardrails for the current v1 contract:

```bash
increase(ticdc_sink_iceberg_committed_ledger_writes_total{result="error"}[5m]) > 0
increase(ticdc_sink_iceberg_committed_ledger_lookups_total{result="error"}[5m]) > 0
ticdc_sink_iceberg_staged_oldest_age_seconds{state="pending"} > <max_expected_drain_seconds>
ticdc_sink_iceberg_staged_bytes{state="pending"} > <max_expected_backlog_bytes>
```

The retention runbook must keep Iceberg snapshots, staged JSON files, and
`.committed` ledger markers for at least the maximum TiCDC replay horizon. If
ledger writes fail after Iceberg append, retained snapshots are still the
fallback until the ledger marker is written on retry.

The fresh replay/hardening scripts can be rerun with:

```bash
local-e2e/run_s03_append_exit_replay.sh
local-e2e/run_s05_stage_exit_replay.sh
local-e2e/run_s09_catalog_outage_recovery.sh
local-e2e/run_s11_append_error_replay.sh
local-e2e/run_s12_high_volume_drain.sh
local-e2e/run_s15_owner_guard.sh
local-e2e/run_s17_create_table_unsupported.sh
local-e2e/run_s17_schema_unsupported.sh
local-e2e/run_s20_rolling_restart.sh
```

The S3-compatible warehouse owner-marker coverage uses the local MinIO service:

```bash
local-e2e/run_s16_minio_owner_marker.sh
```

Final local health checks:

```bash
docker compose -f local-e2e/docker-compose.yml exec -T ticdc-1 \
  /cdc cli capture list --server=http://127.0.0.1:8300

find /tmp/iceberg-warehouse/.ticdc-staging -name '*.json' | wc -l
```

Expected final health:

- three captures up
- no active changefeeds
- staged JSON count `0`
