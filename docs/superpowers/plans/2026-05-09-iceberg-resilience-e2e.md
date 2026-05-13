# Iceberg Resilience E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the TiCDC Iceberg sink is correct across committer failover, writer failover, dependency outage, multi-table and multi-changefeed mappings, high concurrency, and query-engine readback in a local Docker environment.

**Architecture:** Keep the running TiCDC/PID/TiKV/TiDB/Iceberg stack, add PrestoDB, Spark, and Querybook where feasible, then drive scenario-specific changefeeds against unique source tables. A local harness records exact successful DML event counts and validates those counts against Iceberg through the Go readback helper and SQL engines.

**Tech Stack:** Go helpers, Docker Compose, TiCDC new architecture, TiDB v8.5.6 containers, TiCDC v8.5.7-release.1 local image, Iceberg REST catalog, PrestoDB, Spark, Querybook.

---

## File Structure

- `local-e2e/ICEBERG_RESILIENCE_MATRIX.md`: durable scenario checklist.
- `local-e2e/ICEBERG_RESILIENCE_RESULTS.md`: append-only scenario evidence.
- `local-e2e/docker-compose.yml`: local services, including TiCDC, Iceberg REST, PrestoDB, and Spark.
- `local-e2e/presto/etc/catalog/iceberg.properties`: Presto Iceberg REST catalog configuration.
- `local-e2e/spark/conf/spark-defaults.conf`: Spark Iceberg REST catalog configuration.
- `local-e2e/workload/main.go`: high-concurrency TiDB workload generator that prints exact expected CDC event counts.
- `local-e2e/icebergread/main.go`: Go Iceberg readback helper.
- `downstreamadapter/sink/iceberg/sink.go`: optional local fault hooks for deterministic crash-window testing.

## Tasks

- [x] Add PrestoDB and Spark services to Docker Compose and verify both can query `ticdc_iceberg_e2e.orders_cdc`.
- [x] Install Querybook using the official quick setup and verify the UI opens locally.
- [x] Add a workload helper that creates a table, runs concurrent successful writes, and prints expected CDC row counts.
- [x] Add deterministic local fault hooks for the two crash windows that cannot be reliably hit by timing alone.
- [x] Rebuild the Linux TiCDC binary and Docker image after hook changes.
- [x] Run S01 through S28 from `local-e2e/ICEBERG_RESILIENCE_MATRIX.md`.
- [x] For every multi-table or multi-changefeed scenario, verify 1:1, 1:many, many:1, and many:many source-to-Iceberg target mappings.
- [x] For every source table used in correctness scenarios, create enough regions to force multiple TiCDC spans.
- [x] For node-failure scenarios, run separate subcases for `ticdc-1`, `ticdc-2`, and `ticdc-3`, including the current committer and non-committer span writers.
- [x] Record every result in `local-e2e/ICEBERG_RESILIENCE_RESULTS.md`.
- [x] If a scenario finds a correctness bug, mark it as `FAIL` or `KNOWN-LIMITATION`, keep the evidence, then continue with the remaining scenarios.
- [x] Finish with a compact summary of passed, failed, and known-limitation scenarios.
