# Iceberg Sink Production Checkpoint

Date: 2026-05-10

This checkpoint records the current local loop after comparing the prototype
against TiCDC sink patterns, Iceberg commit semantics, and the local resilience
matrix.

## Current Decisions

- The v1 product contract is an append-only CDC log sink. It does not model
  Iceberg as the current TiDB table image.
- Many-changefeed-to-one-Iceberg-target is unsupported for now.
- The sink enforces that decision with both a TiCDC etcd committer lease and a
  shared warehouse target-owner marker. The etcd key is
  `/tidb/cdc/<cluster>/__cdc_meta__/iceberg-committer/<sha256(identifier)>`;
  the warehouse marker is
  `<warehouse>/.ticdc/iceberg-target-owners/<sha256(identifier)>.lock`.
- The owner identity is:
  `cdc-cluster=<ticdc cluster id>;upstream=<upstream TiDB/PD cluster id>;changefeed=<keyspace/changefeed>`.
- The etcd lease handles fast single-cluster failover and release on capture
  session loss. The marker lives in the warehouse, so clusters sharing an
  S3/file warehouse see the same target ownership fence.
- TiCDC also writes owner metadata into Iceberg table properties and snapshot
  properties for audit and defense in depth.
- Target warehouses are locked to local paths, `file://`, and `s3://`
  S3-compatible warehouses. Non-local warehouses require an explicit local/shared
  `staging-dir` while JSON staging remains in this PR. Other cloud warehouse
  schemes are rejected instead of being silently accepted.
- JSON row staging is explicitly bounded to a local/shared filesystem path, such
  as a PVC mounted at the same path on every TiCDC capture. `s3://` is supported
  for the Iceberg warehouse and target-owner marker path, not for native staged
  JSON batch storage. The staging path must provide POSIX file fsync, directory
  fsync, atomic rename, and Bolt-compatible mmap/flock semantics for
  `.committed-row-index`; generic NFS/RWX/object-fuse mounts are unsupported
  unless they explicitly provide those semantics.
- A staged JSON batch is treated as the source-progress durability boundary: it
  is written to a temp file, fsynced, closed, atomically renamed, and then the
  containing target staging directory is fsynced before the writer can
  `PostFlush` upstream progress.
- The committer deduplicates replay through Iceberg snapshot summaries, a
  durable committed-batch ledger under
  `<staging-dir>/<base64(changefeed)>/.committed/<base64(target)>/*.commit`,
  and a durable row-ID segment ledger under `.committed-rows` with Bolt-backed
  exact row-hash shard indexes under `.committed-row-index`. Batch markers,
  row-ID segments, and row-index writes are synced before staged files are
  deleted, and snapshot summary hits backfill missing local markers.
- The replay hot path does not scan retained marker trees into memory. It looks
  up only the candidate staged batch IDs and row IDs currently being drained;
  Iceberg snapshot summaries are scanned from newest to oldest until those batch
  candidates are found. Batch-ledger lookup is direct by candidate marker path,
  and row-ledger lookup reads only the Bolt row-index shards touched by current
  candidate row IDs, so all-new replay misses do not decode retained row
  segments. If a touched shard index is missing or marked dirty after a crash,
  it is rebuilt from retained segment evidence for that shard before answering;
  dirty markers are cleared only after the dirty shard set has been repaired.
  New row-index writes add only the new row hashes to touched shard DBs; they do
  not rewrite cumulative JSON shard files or untouched retained-history shards.
  If a retained segment-only ledger from an older build is present, the first
  lookup reconciles it into the sharded row index before answering.
  The batch and row ledger entry gauges are updated
  incrementally for entries written by the running TiCDC process instead of
  walking the ledger trees on every metrics refresh.
- The ledgers remove snapshot-expiration-only replay risk and delayed
  partial-overlap replay risk when the shared staging directory is retained. The
  remaining runbook guard is: do not purge staged files, `.committed`,
  `.committed-rows`, or `.committed-row-index` markers before the maximum replay
  horizon, and do not expire all relevant Iceberg snapshots while retained
  staged files may still predate the ledger write after an append crash.
- The sink exposes Iceberg operator metrics for staged files/rows/bytes, oldest
  staged age, commit latency/result, committed batch and row counts, durable
  batch and row ledger entries/writes/lookups, staging backend,
  commit-barrier lag, negative commit-barrier lag observations, append failures,
  cleanup failures, and target-owner conflicts.
- Replayed DML row IDs are stable across processor restart splits when TiCDC
  does not populate raw `RowKey`: the sink falls back to the table primary/handle
  key and only uses row index for tables without a usable logical key.
- Partial-overlap replay dedupe is restart-safe even when the later overlapping
  batch is staged after the earlier stage file has already been cleaned up,
  because committed row IDs are kept in the durable row ledger. Staged-file
  cleanup is still deferred until the end of a drain, committed staged evidence
  is retained while the same target has later staged files waiting, and
  duplicate-only replay batches are durably marked handled before cleanup. A
  bounded per-target row-ID cache remains as an in-process optimization, not the
  correctness boundary. The durable long-term answer remains Iceberg-native
  committables with a target-level commit protocol.
- Iceberg schema evolution supports safe ADD COLUMN, RENAME COLUMN, and
  compatible type-widening updates through Iceberg schema transactions.
  DROP COLUMN is conservative: TiCDC stops requiring the column in new row
  payloads, but keeps the existing nullable Iceberg field so historical rows
  remain readable. Live `CREATE TABLE`, `TRUNCATE`, table rename/drop, and
  unsafe type narrowing are explicitly rejected. Bootstrap/not-sync create DDL
  remains allowed.
- Iceberg snapshots use `ticdc.commit-barrier-ts` for the advertised barrier
  contract. The older `ticdc.checkpoint-ts` property is no longer emitted.
- New tables are created from TiDB `TableInfo`, not from the first non-null
  observed row values, and use identity partition fields `dt` and `hr`.

## Latest Local Evidence

- Focused unit and helper packages:
  - `go test ./downstreamadapter/sink/iceberg ./pkg/sink/iceberg ./pkg/metrics ./local-e2e ./local-e2e/workload ./local-e2e/icebergread ./local-e2e/tidbexec -count=1`
  - `go test ./downstreamadapter/sink/iceberg -run 'TestClaimTargetOwnerPublishesEtcdCommitterLease|TestClaimTargetOwnerRunsTakeoverReconciliationOnce|TestClaimTargetOwnerRevalidatesCachedWarehouseMarker|TestCanPromoteIcebergTypeRejectsNarrowingDDL|TestWriteBlockEvent(AppliesSupportedColumnDDL|TreatsIndexDDLAsMetadataOnly)' -count=1`
  - Focused durable-ledger/metrics regression tests cover snapshot-history
    expiration dedupe, delayed partial-overlap replay after cleanup,
    5k-row row-ledger segment/index cardinality, retained-history row miss
    lookup, missing-shard/dirty-shard index reconciliation, untouched retained
    shard index non-rewrite, ledger-before-delete ordering, staged byte/backend
    metrics, committed row metrics, and batch/row ledger write/lookup metrics.
  - Focused review regressions cover staged-file fsync plus parent-directory
    fsync before return, candidate-bounded snapshot-summary dedupe, and
    restart-safe partial-overlap replay dedupe.
- Replay/crash reruns:
  - `S03_REPLAY_PASS cf=s03-replay-1778406697 summary=rows=140 inserts=120 updates=12 deletes=8 staged_after=0 staged_after_remove=0`
  - `S05_REPLAY_PASS cf=s05-replay-1778406656 summary=rows=140 inserts=120 updates=12 deletes=8 staged_after=0 staged_after_remove=0`
  - `S11_REPLAY_PASS cf=s11-replay-1778388072 summary=rows=116 inserts=100 updates=10 deletes=6 staged_after=0 staged_after_remove=0`
  - `S20_ROLLING_PASS cf=s20-restarts-1778391546 summary=rows=700 inserts=600 updates=60 deletes=40 staged_after=0 staged_after_remove=0`
- Earlier metrics scrape after S05 showed `ticdc_sink_iceberg_commit_duration_seconds`,
  `ticdc_sink_iceberg_committed_batches_total`,
  `ticdc_sink_iceberg_committed_rows_total`,
  `ticdc_sink_iceberg_committed_ledger_lookups_total`, and
  `ticdc_sink_iceberg_committed_ledger_writes_total`.
- Catalog and drain reruns:
  - `S09_CATALOG_RECOVERY_PASS cf=s09-catalog-1778388259 summary=rows=583 inserts=500 updates=50 deletes=33 staged_during=19 staged_after=0 staged_after_remove=0`
  - `S12_DRAIN_PASS cf=s12-drain-1778388306 summary=rows=11666 inserts=10000 updates=1000 deletes=666 staged_after=0 staged_after_remove=0`
- Unsupported/owner-guard reruns:
  - `S15_OWNER_PASS cf_left=s15-owner-left-1778391469 cf_right=s15-owner-right-1778391469 summary=rows=70 inserts=60 updates=6 deletes=4 left_state=normal right_state=warning conflicts=9 metric_conflicts=3 staged_during=0 staged_after_remove=0 owner_markers_after_remove=0`
  - `S16_MINIO_OWNER_PASS bucket=ticdc-iceberg-owner prefix=s16-owner-marker-1778388358`
  - `S17_CREATE_TABLE_UNSUPPORTED_PASS cf=s17-create-unsupported-1778391432 rule=ice_s17_create_1778391432.* ddl=CREATE TABLE ice_s17_create_1778391432.orders_created (...) summary=rows=58 inserts=50 updates=5 deletes=3 staged_before_ddl=0 state=warning staged_after_remove=0`
  - The schema-unsupported harness now covers `TRUNCATE`; the previous
    ADD-COLUMN rejection evidence is superseded by positive schema-evolution
    coverage.
- New review-closure local scripts:
  - `local-e2e/run_s17_schema_evolution.sh` verifies safe ADD/RENAME/DROP
    column schema evolution and schema readback.
  - `local-e2e/run_m6_soak.sh` verifies a long-running row-count convergence
    loop, required inserted-row samples, staged backlog cleanup, and the target
    committer lease.
- Metadata check on `ice_s03_replay_1778391385.orders_cdc`:
  - partition spec is `identity(dt)` and `identity(hr)`;
  - snapshots contain `ticdc.commit-barrier-ts`;
  - snapshots do not contain `ticdc.checkpoint-ts`.
- Repeatable local commands:
  `local-e2e/run_s03_append_exit_replay.sh`,
  `local-e2e/run_s05_stage_exit_replay.sh`,
  `local-e2e/run_s09_catalog_outage_recovery.sh`,
  `local-e2e/run_s11_append_error_replay.sh`,
  `local-e2e/run_s12_high_volume_drain.sh`,
  `local-e2e/run_s15_owner_guard.sh`,
  `local-e2e/run_s16_minio_owner_marker.sh`,
  `local-e2e/run_s17_schema_evolution.sh`,
  `local-e2e/run_s17_create_table_unsupported.sh`,
  `local-e2e/run_s17_schema_unsupported.sh`, and
  `local-e2e/run_s20_rolling_restart.sh`. The longer soak command is
  `local-e2e/run_m6_soak.sh`.

## Remaining Production Hardening Loop

- Replace JSON row staging with Iceberg-native data-file committables. Reuse the
  mature cloud/external-storage writer path where possible instead of inventing a
  separate high-throughput file pipeline. This is still the next architecture
  loop before calling the sink production-shape for high throughput.
- Add a real per-Iceberg-target data-file commit coordinator if many-source or
  many-changefeed-to-one-target must become supported. Until then, keep the
  etcd lease plus warehouse owner marker as a hard rejection.
- Add alert rules, dashboard panels, and a runbook on top of the new metrics.
  The minimum alerts for this PR are non-zero
  `ticdc_sink_iceberg_committed_ledger_writes_total{result="error"}`,
  non-zero `ticdc_sink_iceberg_committed_ledger_lookups_total{result="error"}`,
  non-zero `ticdc_sink_iceberg_committed_row_ledger_writes_total{result="error"}`,
  non-zero `ticdc_sink_iceberg_committed_row_ledger_lookups_total{result="error"}`,
  stale or growing staged backlog age/bytes, and retention-policy drift where
  Iceberg snapshots, staged JSON files, `.committed`, `.committed-rows`, or
  `.committed-row-index` markers are purged before the maximum replay horizon.
  Still missing explicit catalog retry counters and
  table-creation/schema-failure counters.
- Extend MinIO/S3-style integration coverage beyond the owner marker to include
  credential/auth failure modes, remote orphan cleanup, and eventually
  remote/native data-file staging. The current S3 support is the warehouse path
  and owner marker; staged JSON remains shared filesystem/PVC only.
- Add deployment validation/runbook checks for the staging filesystem contract:
  identical mount path on every capture, durable file and directory fsync,
  atomic rename, and Bolt-compatible mmap/flock locking.
- Expand local scale tests by simulating thousands of changefeeds with many small
  tables, aggressive checkpoint cadence, catalog outage/recovery, and staged
  backlog drain limits. A single laptop cannot prove 30GB/s, but it can catch
  algorithmic O(N) and unbounded backlog behavior.
- Keep table lifecycle DDL unsupported until the DDL barrier and target-level
  commit protocol cover create/truncate/rename/drop semantics. Current schema
  evolution support is intentionally limited to safe column changes.

## Design Comparison Notes

- TiCDC's cloud-storage sink already has useful pieces to reuse: external
  storage abstraction, multipart `Create`/`Write`/`Close`, schema-version files,
  per-worker flush metrics, defragmented ordering, and cleanup logic. The
  Iceberg sink should avoid a separate object-store writer pipeline where those
  pieces can be adapted.
- Apache Iceberg's Flink sink makes the key split explicit: parallel writers
  flush data/delete files during checkpoints, and a committer commits those
  files after checkpoint completion. Its official metrics separate writer flush
  duration/file counts from committer checkpoint/commit duration and last
  successful commit lag:
  https://iceberg.apache.org/docs/latest/docs/flink-writes/
- Apache Iceberg's Kafka Connect sink advertises centralized Iceberg commit
  coordination, exactly-once delivery, multi-table fan-out, automatic table
  creation, and schema evolution. That reinforces that TiCDC should not support
  many-to-one Iceberg targets without a real target-level coordinator:
  https://iceberg.apache.org/docs/1.7.2/kafka-connect/
- Delta Lake's streaming docs call out the same duplicate-write failure mode and
  solve it with a durable `(txnAppId, txnVersion)` identity. TiCDC's current
  Iceberg snapshot batch IDs are the local equivalent and should become a
  first-class commit ledger if/when JSON staging is replaced:
  https://docs.delta.io/delta-streaming/
