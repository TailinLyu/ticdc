# Iceberg Sink Production Checkpoint

Date: 2026-05-10

This checkpoint records the current local loop after comparing the prototype
against TiCDC sink patterns, Iceberg commit semantics, and the local resilience
matrix.

## Current Decisions

- Many-changefeed-to-one-Iceberg-target is unsupported for now.
- The sink enforces that decision with a shared warehouse target-owner marker:
  `<warehouse>/.ticdc/iceberg-target-owners/<sha256(identifier)>.lock`.
- The owner identity is:
  `cdc-cluster=<ticdc cluster id>;upstream=<upstream TiDB/PD cluster id>;changefeed=<keyspace/changefeed>`.
- The marker lives in the warehouse, so clusters sharing an S3/file warehouse see
  the same target ownership fence.
- TiCDC also writes owner metadata into Iceberg table properties and snapshot
  properties for audit and defense in depth.
- The sink exposes initial Iceberg operator metrics for staged backlog and
  target-owner conflicts.
- The external-storage timeout wrapper preserves the underlying strong
  consistency marker, so S3/GCS/Azure-backed owner locks keep the consistency
  signal exposed by TiDB's storage layer.
- Replayed DML row IDs are stable across processor restart splits when TiCDC
  does not populate raw `RowKey`: the sink falls back to the table primary/handle
  key and only uses row index for tables without a usable logical key.
- Iceberg schema evolution DDL is explicitly unsupported for now. The sink fails
  the changefeed instead of silently producing partial ADD/DROP/RENAME/TRUNCATE
  semantics.

## Latest Local Evidence

- Replay/crash reruns:
  - `S03_REPLAY_PASS cf=s03-replay-1778388027 summary=rows=350 inserts=300 updates=30 deletes=20 staged_after=0 staged_after_remove=0`
  - `S05_REPLAY_PASS cf=s05-replay-1778387908 summary=rows=350 inserts=300 updates=30 deletes=20 staged_after=0 staged_after_remove=0`
  - `S11_REPLAY_PASS cf=s11-replay-1778388072 summary=rows=116 inserts=100 updates=10 deletes=6 staged_after=0 staged_after_remove=0`
  - `S20_ROLLING_PASS cf=s20-restarts-1778387817 summary=rows=2333 inserts=2000 updates=200 deletes=133 staged_after=0 staged_after_remove=0`
- Catalog and drain reruns:
  - `S09_CATALOG_RECOVERY_PASS cf=s09-catalog-1778388259 summary=rows=583 inserts=500 updates=50 deletes=33 staged_during=19 staged_after=0 staged_after_remove=0`
  - `S12_DRAIN_PASS cf=s12-drain-1778388306 summary=rows=11666 inserts=10000 updates=1000 deletes=666 staged_after=0 staged_after_remove=0`
- Unsupported/owner-guard reruns:
  - `S15_OWNER_PASS cf_left=s15-owner-left-1778388335 cf_right=s15-owner-right-1778388335 summary=rows=116 inserts=100 updates=10 deletes=6 left_state=warning right_state=normal conflicts=5 metric_conflicts=1 staged_during=7 staged_after_remove=0 owner_markers_after_remove=0`
  - `S16_MINIO_OWNER_PASS bucket=ticdc-iceberg-owner prefix=s16-owner-marker-1778388358`
  - `S17_SCHEMA_UNSUPPORTED_PASS cf=s17-unsupported-1778388281 summary=rows=58 inserts=50 updates=5 deletes=3 staged_before_ddl=0 state=warning staged_after_remove=0`
- Repeatable local commands:
  `local-e2e/run_s03_append_exit_replay.sh`,
  `local-e2e/run_s05_stage_exit_replay.sh`,
  `local-e2e/run_s09_catalog_outage_recovery.sh`,
  `local-e2e/run_s11_append_error_replay.sh`,
  `local-e2e/run_s12_high_volume_drain.sh`,
  `local-e2e/run_s15_owner_guard.sh`,
  `local-e2e/run_s16_minio_owner_marker.sh`,
  `local-e2e/run_s17_schema_unsupported.sh`, and
  `local-e2e/run_s20_rolling_restart.sh`.

## Remaining Production Hardening Loop

- Replace JSON row staging with Iceberg-native data-file committables. Reuse the
  mature cloud/external-storage writer path where possible instead of inventing a
  separate high-throughput file pipeline.
- Add a real per-Iceberg-target commit coordinator if many-source or
  many-changefeed-to-one-target must become supported. Until then, keep the
  warehouse owner marker as a hard rejection.
- Expand metrics and alerts beyond the initial staged-backlog and target-owner
  conflict signals to include committed batches, append latency, catalog retries,
  checkpoint lag, and cleanup failures.
- Extend MinIO/S3-style integration coverage beyond the owner marker to include
  staged-file cleanup and eventually remote/native data-file staging.
- Expand local scale tests by simulating thousands of changefeeds with many small
  tables, aggressive checkpoint cadence, catalog outage/recovery, and staged
  backlog drain limits. A single laptop cannot prove 30GB/s, but it can catch
  algorithmic O(N) and unbounded backlog behavior.
- Keep ADD/DROP/RENAME/TRUNCATE DDL unsupported until the DDL barrier and Iceberg
  schema-update protocol are implemented. The current local check verifies the
  rejection path, not schema evolution support.

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
