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

## Latest Local Evidence

- Same-target S15 rerun:
  `S15_OWNER_PASS cf_left=s15-owner-left-1778379359 cf_right=s15-owner-right-1778379359 summary=rows=116 inserts=100 updates=10 deletes=6 left_state=normal right_state=warning conflicts=5 metric_conflicts=1 staged_during=7 staged_after_remove=0 owner_markers_after_remove=0`
- Repeatable local command:
  `local-e2e/run_s15_owner_guard.sh`
- MinIO/S3-compatible owner-marker rerun:
  `S16_MINIO_OWNER_PASS bucket=ticdc-iceberg-owner prefix=s16-owner-marker-1778378985`
- Repeatable local command:
  `local-e2e/run_s16_minio_owner_marker.sh`
- Focused verification:
  `go test ./coordinator ./downstreamadapter/sink ./downstreamadapter/sink/iceberg ./pkg/sink/iceberg ./pkg/config ./pkg/metrics ./pkg/util ./local-e2e ./local-e2e/workload ./local-e2e/icebergread ./local-e2e/tidbexec -count=1`

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
- Finish schema-evolution semantics for ADD/DROP/RENAME/TRUNCATE instead of
  treating the sink as only an append-log row-count target.

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
