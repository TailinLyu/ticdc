# TiCDC Iceberg Resilience Matrix

This file is the durable checklist for the local Iceberg sink validation. It is
kept in the repo so work can resume after context compaction without relying on
conversation memory.

## Query Engines And UI

- [x] Install and verify PrestoDB against the local Iceberg REST catalog.
- [x] Install and verify Spark against the local Iceberg REST catalog.
- [x] Install and open Querybook for query visualization.
- [x] Connect Querybook to the PrestoDB engine if the UI supports it locally.

## Correctness Invariants

Every data scenario must verify all of these:

Checked means the invariant was exercised and recorded. Scenarios with expected
failures or limitations are documented in `ICEBERG_RESILIENCE_RESULTS.md`.

- [x] The changefeed returns to `normal`.
- [x] Exactly one TiCDC process logs `iceberg committer enabled` for the changefeed at any moment.
- [x] Multiple TiCDC processes can log `iceberg writer staged rows` when the table has multiple spans.
- [x] Only the active committer logs `iceberg committer appended staged rows`.
- [x] `/tmp/iceberg-warehouse/.ticdc-staging` has zero `.json` files after the changefeed catches up.
- [x] Iceberg row counts match the exact expected number of successful insert, update, and delete row events.
- [x] Query verification works through the Go helper and at least one SQL engine.
- [x] Every source table in the scenario has multiple TiKV regions and multiple TiCDC spans.
- [x] Span dispatchers for each table are distributed across more than one TiCDC node unless the scenario intentionally kills nodes.
- [x] Failure-mode scenarios cover each TiCDC node role: current committer/table-trigger owner, non-committer writer, and replacement committer.

## Scenario List

### P0: Failover And No-Loss / No-Duplicate Windows

- [x] S01: committer hard-killed during active writes.
- [x] S02: committer hard-killed after writers stage files but before drain.
- [x] S03: committer hard-killed after Iceberg append but before deleting the staged file.
- [x] S04: non-committer writer hard-killed during active writes before it can stage some buffered rows.
- [x] S05: non-committer writer hard-killed after staging but before `PostFlush`.

### P1: Recovery And Dependency Failures

- [x] S06: graceful committer restart.
- [x] S07: all non-committer writers killed while committer survives.
- [x] S08: all TiCDC containers killed, then restarted.
- [x] S09: Iceberg REST outage while staged files exist, then REST recovery.
- [x] S10: warehouse/staging path write failure.
- [x] S11: staged-file delete failure after successful append.

### P2: Scale, Scheduling, Multi-Table, And Semantics

- [x] S12: high-volume staged-batch drain.
- [x] S13: multiple source tables in one changefeed.
- [x] S14: two changefeeds writing different Iceberg target tables.
- [x] S15: two changefeeds writing the same Iceberg target table.
- [x] S16: move/split/merge table while writing.
- [x] S17: DDL behavior: add column, drop column, rename table, truncate table.
- [x] S18: out-of-order commit timestamps across spans.
- [x] S19: duplicate detection/idempotency under replay.
- [x] S20: long-running random TiCDC restart loop.
- [x] S21: high-concurrency workload with many rows emitted per millisecond.
- [x] S22: 1:1 mapping, one source table to one Iceberg table, with multiple spans.
- [x] S23: 1:many mapping, one source table replicated by multiple changefeeds to multiple Iceberg target tables.
- [x] S24: many:1 mapping, multiple changefeeds or source tables targeting one Iceberg table.
- [x] S25: many:many mapping, multiple source tables and multiple changefeeds targeting multiple Iceberg tables.
- [x] S26: node-by-node committer failover matrix: kill `ticdc-1`, `ticdc-2`, and `ticdc-3` in separate runs and verify committer replacement.
- [x] S27: node-by-node span writer failover matrix: kill each node while it owns table spans and verify all spans reschedule and no data is lost.
- [x] S28: per-table committer ownership in multiple-changefeed setup, verifying each changefeed has exactly one committer and no cross-changefeed drain.

## Expected Result Format

Each scenario should append a result block to `local-e2e/ICEBERG_RESILIENCE_RESULTS.md`:

```text
## SXX: Name
status: PASS | FAIL | KNOWN-LIMITATION
changefeed: <id>
source_table: <db>.<table>
expected_rows: inserts=<n> updates=<n> deletes=<n> total=<n>
go_readback: rows=<n> inserts=<n> updates=<n> deletes=<n>
sql_readback: <engine and result>
stage_files_remaining: <n>
committer_history: <service list>
writer_history: <service list>
span_distribution: <table -> service counts>
notes: <short explanation>
```
