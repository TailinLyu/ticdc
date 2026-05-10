# Iceberg E2E Current Non-Green And Resolved Results

This file records the local S01-S28 matrix rows that were non-green before the
latest idempotency and hardening loop, plus their current status after fresh
local reruns.

Full evidence is in `local-e2e/ICEBERG_RESILIENCE_RESULTS.md`.

## Current Non-Green Scenarios

| ID | Status | What failed | Evidence | Root cause / interpretation |
| --- | --- | --- | --- | --- |
| S15 | UNSUPPORTED / REJECTED | Two changefeeds writing the same Iceberg target table. | Fresh rebuilt-image run: `S15_OWNER_PASS cf_left=s15-owner-left-1778388335 cf_right=s15-owner-right-1778388335 summary=rows=116 inserts=100 updates=10 deletes=6 left_state=warning right_state=normal conflicts=5 metric_conflicts=1 staged_during=7 staged_after_remove=0 owner_markers_after_remove=0`. | The first owner claims the target in the shared warehouse; the second owner is rejected before append. This intentionally bans many-changefeed-to-one-target, including across clusters sharing S3/file warehouse storage. |
| S24 | UNSUPPORTED / CONFIG_LIMITATION | many:1 mapping into one Iceberg target. | Same-target changefeeds are now rejected by the shared warehouse owner marker; different source table names still cannot be routed to one Iceberg table by current config. | Supporting true many-source-table-to-one-target would require target-identifier override/routing plus a target-table-level commit protocol. Until then, this shape is explicitly unsupported. |

## Current Partial Or Semantic-Limit Scenarios

| ID | Status | What was limited | Evidence | Interpretation |
| --- | --- | --- | --- | --- |
| S16 | PARTIAL | Move/split/merge scheduler operations while writing. | Split and merge succeeded; `move-split-table` returned `ErrOperatorIsNil`; after merge to one replication, `move-table` succeeded and counts stayed exact. | Data correctness was fine, but one scheduler operation did not work in this local setup. |
| S17 | UNSUPPORTED / REJECTED | Iceberg schema evolution DDL. | Fresh rebuilt-image run: `S17_SCHEMA_UNSUPPORTED_PASS cf=s17-unsupported-1778388281 summary=rows=58 inserts=50 updates=5 deletes=3 staged_before_ddl=0 state=warning staged_after_remove=0`. | ADD/DROP/RENAME/TRUNCATE style DDL is now treated as unsupported instead of silently producing partial Iceberg schema semantics. Production schema evolution still requires a DDL barrier and Iceberg schema-update protocol. |

## Resolved In Current Loop

| ID | Status | Fresh local evidence | Fix / interpretation |
| --- | --- | --- | --- |
| S03 | PASS | `S03_REPLAY_PASS cf=s03-replay-1778388027 summary=rows=350 inserts=300 updates=30 deletes=20 staged_after=0 staged_after_remove=0` | Retained staged files are now skipped by committed batch IDs, and replayed keyed rows use stable table-key row IDs. |
| S05 | PASS | `S05_REPLAY_PASS cf=s05-replay-1778387908 summary=rows=350 inserts=300 updates=30 deletes=20 staged_after=0 staged_after_remove=0` | Durable stage plus unacked upstream replay no longer duplicates rows. |
| S09 | PASS | `S09_CATALOG_RECOVERY_PASS cf=s09-catalog-1778388259 summary=rows=583 inserts=500 updates=50 deletes=33 staged_during=19 staged_after=0 staged_after_remove=0` | Catalog recovery now drains automatically after REST returns; no TiCDC restart required in the local replay. |
| S11 | PASS | `S11_REPLAY_PASS cf=s11-replay-1778388072 summary=rows=116 inserts=100 updates=10 deletes=6 staged_after=0 staged_after_remove=0` | Append succeeded but cleanup failed is idempotent on retry. |
| S12 | PASS | `S12_DRAIN_PASS cf=s12-drain-1778388306 summary=rows=11666 inserts=10000 updates=1000 deletes=666 staged_after=0 staged_after_remove=0` | The local high-volume drain now completes exactly at the prior stress size. |
| S19 | PASS | Covered by fresh S03/S05/S11 replay windows. | Duplicate replay checks are now represented by repeatable local scripts. |
| S20 | PASS | `S20_ROLLING_PASS cf=s20-restarts-1778387817 summary=rows=2333 inserts=2000 updates=200 deletes=133 staged_after=0 staged_after_remove=0` | Rolling hard restarts no longer duplicate keyed rows after row-ID fallback was stabilized. |

## Highest Priority Follow-Up

The remaining design compromise is JSON row staging. The next production design
should replace it with Iceberg-native committables:

- parallel writers produce data-file committables rather than JSON batches,
- one target-level committer atomically commits those files to Iceberg,
- unsupported many-to-one targets stay rejected until that coordinator exists,
- schema evolution remains unsupported until a DDL barrier and Iceberg schema
  update protocol are implemented.
