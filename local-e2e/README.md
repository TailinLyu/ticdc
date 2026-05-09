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
make failpoint-enable
GOOS=linux GOARCH=arm64 go build -o bin/cdc-linux-arm64 ./cmd/cdc
make failpoint-disable

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

The matrix is intentionally not all green. The non-green rows are documented in
`ICEBERG_RESILIENCE_RESULTS.md` and summarized in `TEST_FAILURES.md`.

Main correctness gaps:

- A staged batch can be appended to Iceberg and then appended again after
  committer failure if the staged file survives append. This affects S03, S11,
  S19, and appears in rolling restart S20.
- A writer can stage a file, fail before `PostFlush`, and replay the same event
  from upstream while the durable staged file is also drained. This affects S05.
- Committer uniqueness is scoped to one changefeed, not to one Iceberg target
  table. Two changefeeds writing the same target duplicate the stream. This
  affects S15 and S24.
- Iceberg REST DNS/catalog outage did not recover automatically after REST came
  back; TiCDC restart drained the backlog. This affects S09.
- The local Tabulario REST catalog uses a SQLite/JDBC backend and became the
  bottleneck under the largest staged-drain stress run. This affects S12.
- DDL row counts are correct, but Iceberg schema evolution is limited. Added
  columns did not appear in the existing Iceberg `data` struct, dropped columns
  remained nullable, rename maps to a new target, and truncate does not remove
  old Iceberg rows. This affects S17.

The highest-priority follow-up is an idempotency protocol: stable batch IDs plus
a durable committed-batch marker or equivalent target-table commit ledger, so a
replacement committer can distinguish "already appended" from "not yet
appended" before draining retained staged files.

## Verification Commands

Fresh verification used before opening the PR:

```bash
go test ./downstreamadapter/sink/iceberg ./pkg/sink/iceberg \
  ./local-e2e/workload ./local-e2e/icebergread ./local-e2e/tidbexec -count=1
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
