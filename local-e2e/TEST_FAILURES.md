# Iceberg E2E Non-Green Test Results

This file intentionally documents the non-green local E2E results from the
S01-S28 matrix. These are not hidden CI failures; they are the correctness gaps
found by the local validation harness.

Full evidence is in `local-e2e/ICEBERG_RESILIENCE_RESULTS.md`.

## Failing Scenarios

| ID | Status | What failed | Evidence | Root cause / interpretation |
| --- | --- | --- | --- | --- |
| S03 | FAIL | Committer exits after Iceberg append but before staged-file delete. | Expected `rows=350 inserts=300 updates=30 deletes=20`; actual `rows=370 inserts=320 updates=30 deletes=20`. | A staged batch can be appended to Iceberg, survive on disk, and be appended again by the replacement committer. |
| S05 | FAIL | Non-committer writer exits after staging but before `PostFlush`. | Expected `rows=350 inserts=300 updates=30 deletes=20`; actual `rows=370 inserts=320 updates=30 deletes=20`. | Durable staged file plus unacked upstream event can both be replayed into Iceberg. |
| S11 | FAIL | Staged-file delete fails after a successful append. | Expected `rows=116 inserts=100 updates=10 deletes=6`; actual `rows=126 inserts=110 updates=10 deletes=6`. | Same idempotency gap as S03: append succeeded but retained staged file was appended again. |
| S12 | FAIL | High-volume staged-batch drain under the local Iceberg REST catalog. | Expected `rows=11666`; actual stalled at `rows=7253 inserts=6782 updates=284 deletes=187`; staged files reached `782`; changefeed stayed `warning`. | The local `tabulario/iceberg-rest` SQLite/JDBC catalog hit catalog-lock/backpressure limits and TiCDC did not drain automatically. |
| S15 | FAIL | Two changefeeds writing the same Iceberg target table. | One source stream had `rows=116`; final target readback was `rows=232 inserts=200 updates=20 deletes=12`. | Committer uniqueness is scoped per changefeed, not per Iceberg target table. Two independent committers duplicated the stream. |
| S19 | FAIL | Duplicate detection/idempotency under replay. | Covered by S03, S05, and S11 duplicate readbacks. | There is no durable committed-batch marker or exactly-once staged-file protocol yet. |
| S20 | FAIL | Long-running rolling hard restart loop. | Expected `rows=2333 inserts=2000 updates=200 deletes=133`; actual `rows=3110 inserts=2667 updates=266 deletes=177`. | Reproduces the replay/idempotency duplication without deterministic failpoints. |
| S24 | FAIL / CONFIG_LIMITATION | many:1 mapping into one Iceberg target. | Same-target case duplicated rows in S15; different source table names cannot be routed to one Iceberg table by current config. | Needs target-identifier override/routing plus a target-table-level committer protocol. |

## Partial Or Semantic-Limit Scenarios

| ID | Status | What was limited | Evidence | Interpretation |
| --- | --- | --- | --- | --- |
| S09 | PARTIAL | Iceberg REST outage recovery. | Staged files remained while REST was down; REST restart alone did not drain; TiCDC restart drained exactly to `rows=583 inserts=500 updates=50 deletes=33`. | No data loss, but automatic recovery after catalog DNS outage needs reconnect/retry cleanup. |
| S16 | PARTIAL | Move/split/merge scheduler operations while writing. | Split and merge succeeded; `move-split-table` returned `ErrOperatorIsNil`; after merge to one replication, `move-table` succeeded and counts stayed exact. | Data correctness was fine, but one scheduler operation did not work in this local setup. |
| S17 | PASS_WITH_SEMANTIC_LIMITS | DDL schema semantics. | Row counts were correct, but added columns did not appear in the existing Iceberg `data` struct, dropped columns stayed nullable, rename created a new target table, and truncate did not remove old rows. | Counts are correct for append-log behavior; Iceberg schema evolution semantics are incomplete. |

## Highest Priority Follow-Up

The main correctness hole is the staged-batch idempotency protocol.

A robust fix needs stable batch identity plus a durable "already committed"
record, such as:

- a committed-batch ledger keyed by changefeed/table/batch ID,
- an Iceberg snapshot-property or metadata-file protocol that can be checked
  before re-appending retained staged files, or
- an equivalent target-table-level commit marker that survives committer death.

Without that, replacement committers cannot reliably distinguish "staged but not
appended" from "appended but cleanup failed."
