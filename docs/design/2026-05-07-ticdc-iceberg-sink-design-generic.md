# TiCDC Native Iceberg Sink — Implementation Spec (Local-First)

**Audience**: an autonomous coding agent implementing this end-to-end against a local TiDB cluster, a local Iceberg REST catalog, and a local filesystem warehouse. All references to cloud services, internal infrastructure, and proprietary buckets are intentionally absent. The agent should be able to work in a tight code → build → test → iterate loop without external dependencies.

**Status**: implementation spec
**Date**: 2026-05-07
**Targets**: TiCDC new-arch (v8.5.x and forward), Apache Iceberg format-version 2, iceberg-go HEAD

---

## 0. Quick orientation for the implementing agent

Before writing any code, set up the local environment described in §6, then validate it with the smoke test described in §6.6. The implementation work lives in the TiCDC source tree and is structured as additive packages — see §7.5 for the new-package list and §8 for the small additive edits to existing files.

The implementation is best done in milestones (§17). Each milestone has an acceptance test the agent can run locally to declare it "done." Do not advance past a milestone until its acceptance test passes.

When confused about a TiCDC internal, read the actual code path. Key files:
- `downstreamadapter/sink/sink.go` — the `Sink` interface and dispatch switch
- `downstreamadapter/sink/cloudstorage/` — the closest existing analog for batch-output sinks
- `downstreamadapter/dispatcher/basic_dispatcher.go` — the Dispatcher protocol (DDL barriers, PostFlush, checkpoint advancement)
- `pkg/sink/codec/csv/csv_message.go` — TiDB-side type decoding logic we lift
- `server/module_election.go` — etcd-based leader election we copy the pattern from
- `pkg/etcd/etcd.go`, `pkg/etcd/etcdkey.go` — etcd usage patterns
- `pkg/messaging/` — internal RPC

---

## 1. What we're building

A new TiCDC sink scheme `iceberg://` that writes change-data-capture events into Apache Iceberg tables in append-only CDC log shape. The sink writes Parquet data files directly to the configured warehouse, then commits batches of files into Iceberg snapshots on a configurable cadence (default 60 seconds).

The committed Iceberg tables have a fixed schema shape:
- Synthetic columns: `_op` (I/U/D), `_commit_ts` (TSO), `_commit_dt` (timestamp), `_start_ts`, `_seq`, `_table_id`, `dt` (date partition), `hr` (hour partition).
- Two nested struct columns: `data` (post-image, NULL for Delete) and `old` (pre-image, NULL for Insert).

The sink supports TiCDC's existing single-table-split-across-N-nodes mode (`enable-table-across-nodes=true`). In that mode, multiple writers contribute Parquet files to the same Iceberg table in parallel, but only one node holds the commit role at a time, elected via etcd. Single-writer-per-Iceberg-table CAS guarantees consistent commits.

## 2. Why

Streaming pipelines that go TiDB → TiCDC → Kafka → Flink → Iceberg have several recurring problems: an extra service to operate (Kafka), a stateful streaming compute layer (Flink), event-time watermark heuristics that cannot guarantee partition completeness, and high transport cost. A native sink eliminates the intermediate stages and provides a hard mathematical guarantee on partition finalization through a recorded commit-barrier-TS.

## 3. Goals

1. **Streaming CDC log tables in Iceberg**: TiCDC writes Iceberg CDC log tables directly, with sub-minute-latency freshness, no intermediate streaming engine.
2. **Multi-node parallelism**: leverage existing TiCDC `enable-table-across-nodes` span splitting; write Parquet from N writers in parallel; commit through a single committer-per-Iceberg-table elected via etcd.
3. **Hard partition finalization guarantee**: snapshot summaries record `ticdc.commit-barrier-ts`. Any consumer can know with certainty that "all rows with `commit_ts <= barrier` are present" by reading this single snapshot property.
4. **No new infrastructure dependencies**: reuse PD-etcd (already required by TiCDC), reuse iceberg-go (existing library), reuse the configured Iceberg catalog (REST or SQL). The sink adds zero new daemons, sidecars, or coordination services.
5. **Pure extension over TiCDC**: net-new code lives in new packages. Modifications to existing files are limited to scheme registration in the dispatch switch, a URL scheme constant, three new message types, and one new etcd key prefix. Feature-flag-off is byte-identical to stock TiCDC.

## 4. Non-goals

1. **Replacing batch base-table writers** (Spark MERGE, Trino INSERT, etc.). MoR / streaming-merge approaches don't perform at TB scale; mandatory compaction reintroduces the same compute cost. We replace transport, not materialization.
2. **Merge-on-read base tables.** Append-only CDC log only.
3. **Real-time consumer-facing freshness below ~30s.** Default commit cadence is 60s; configurable down to 15s; below that, small-file pain dominates.
4. **Partition spec other than `(dt, hr)` derived from `_commit_ts`.** Bucketed and hash partition specs are out of scope for v1.
5. **Per-column type overrides.** v1 emits Iceberg types via the standard TiDB→Iceberg mapping table (§9.3), with widest-type-day-1 policy for integer types. Per-column overrides are a v2 concern.
6. **Catalog implementations beyond REST and SQL.** v1 targets these two. Hive Metastore, AWS Glue, Hadoop file catalog, and others are out of scope.

## 5. Out of scope for v1 (sink halts on encounter)

These DDL events are explicitly NOT supported in v1. When the sink encounters them, it returns an error from `WriteBlockEvent`, the changefeed enters an error state, and a human operator must intervene. **Halting is acceptable. Silent skip is not.**

1. **`TRUNCATE TABLE`**: semantics for an append-only CDC log are unclear (preserve history? emit synthetic deletes?). Defer.
2. **`CREATE TABLE`** for new tables matching the changefeed filter: operators must pre-create the Iceberg table.
3. **`DROP TABLE`**: operators must clean up the Iceberg table manually.

The bootstrap utility in §16 covers pre-creation.

## 6. Local development setup

The implementing agent should perform all development and acceptance testing against this local-only stack. No cloud, no internal services, no network beyond `localhost`.

### 6.1 Prerequisites

The agent's host machine must have:

- Go 1.25+
- Docker 24+
- k3d 5.7+
- kubectl 1.30+
- helm 3.x
- python3.9+
- A working TiCDC source checkout (or this monorepo with `ticdc/` subdirectory)

**Apple Silicon Macbook (M-series, ARM64) notes**: all images we use publish multi-arch manifests (PingCAP `pingcap/{pd,tidb,tikv,ticdc}`, `tabulario/iceberg-rest`, `pingcap/tidb-operator`), so Docker Desktop pulls the arm64 variant automatically. Memory budget on a Macbook: TiDB+TiKV+PD+TiCDC under k3d uses ~3-4 GiB; iceberg-rest adds ~500 MiB; Docker Desktop's Linux VM overhead is ~2 GiB. **Allocate at least 10 GiB to Docker Desktop** (16 GiB Macbook minimum, 32 GiB comfortable). Configure via Docker Desktop → Settings → Resources → Memory.

The agent verifies prerequisites with:

```bash
go version           # expect go1.25.x or later
docker version       # expect 24.x or later
k3d version
kubectl version --client
helm version
```

### 6.2 Local TiDB cluster (k3d)

Use the `local-dev/cli.py` helper in this repo to bring up a single-node TiDB+TiCDC cluster on k3d. This deploys PD, TiKV, TiDB, and TiCDC behind the existing `tidb-operator`, with NodePort mappings for SQL (4000), PD API (2379), and TiCDC's API.

```bash
cd <repo-root>
python3 local-dev/cli.py setup --version v8.5.6 --local-components ticdc --rebuild
```

`--local-components ticdc` rebuilds TiCDC from the local source tree on every invocation, importing the resulting `ticdc-local:v8.5.6` image into k3d. Other components (PD, TiKV, TiDB) come from public PingCAP images.

Verification:

```bash
mysql -h 127.0.0.1 -P 4000 -u root -e 'SELECT tidb_version()'
kubectl -n tidb-local get pods -o custom-columns='NAME:.metadata.name,IMAGE:.spec.containers[*].image'
# The ticdc pod's image should be ticdc-local:v8.5.6.
```

### 6.3 Local Iceberg REST catalog

The agent runs `tabulario/iceberg-rest:latest`, which implements the Apache Iceberg REST Catalog Open API spec — the same spec that production REST catalogs (Tabular, Polaris, Gravitino, AWS Glue REST mode) implement. The iceberg-go REST client is identical regardless of backend; code that works against the local catalog runs unchanged against any spec-conformant catalog with at most a few config-line additions for auth/headers.

**Two deployment options for the local catalog**, depending on the agent's host OS:

**Option A (recommended for macOS Macbook agents): in-cluster iceberg-rest.**

On macOS, Docker Desktop runs a Linux VM, so `hostNetwork: true` on a k3d pod attaches to the VM's network, not the Mac's. The pod can't reach a `localhost:8181` running on the Mac. Solution: run iceberg-rest inside k3d as a Pod, exposed via a Service.

```yaml
# kubectl apply -f the following
apiVersion: v1
kind: Service
metadata:
  name: iceberg-rest
  namespace: tidb-local
spec:
  ports:
    - port: 8181
      targetPort: 8181
  selector:
    app: iceberg-rest
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: iceberg-rest
  namespace: tidb-local
spec:
  replicas: 1
  selector:
    matchLabels:
      app: iceberg-rest
  template:
    metadata:
      labels:
        app: iceberg-rest
    spec:
      containers:
        - name: iceberg-rest
          image: tabulario/iceberg-rest:latest
          ports:
            - containerPort: 8181
          env:
            - name: CATALOG_WAREHOUSE
              value: file:///iceberg-warehouse
            - name: CATALOG_URI
              value: jdbc:sqlite:/catalog/catalog.db
            - name: CATALOG_JDBC_USER
              value: u
            - name: CATALOG_JDBC_PASSWORD
              value: p
            - name: CATALOG_IO__IMPL
              value: org.apache.iceberg.hadoop.HadoopFileIO
          volumeMounts:
            - name: warehouse
              mountPath: /iceberg-warehouse
            - name: catalog-db
              mountPath: /catalog
      volumes:
        - name: warehouse
          persistentVolumeClaim:
            claimName: iceberg-warehouse-pvc
        - name: catalog-db
          emptyDir: {}                # SQLite catalog; resets on pod restart, OK for dev
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: iceberg-warehouse-pvc
  namespace: tidb-local
spec:
  accessModes: [ReadWriteMany]
  storageClassName: local-path
  resources:
    requests:
      storage: 5Gi
```

Both TiCDC and iceberg-rest mount the same PVC — TiCDC via `additionalVolumes` on the TidbCluster CRD, iceberg-rest via the Deployment above. The TiCDC pod accesses iceberg-rest at `http://iceberg-rest.tidb-local.svc.cluster.local:8181` and writes Parquet to `/iceberg-warehouse/...`.

To expose the catalog to the Mac host (for the agent's bootstrap utility, validation queries, etc.):

```bash
kubectl -n tidb-local port-forward svc/iceberg-rest 8181:8181
```

**Option B (Linux hosts only): standalone Docker container on the host.**

```bash
mkdir -p /tmp/iceberg-warehouse
docker run -d --name iceberg-rest \
  -p 8181:8181 \
  -v /tmp/iceberg-warehouse:/tmp/iceberg-warehouse \
  -v $HOME/.iceberg-rest-catalog.db:/catalog/catalog.db \
  -e CATALOG_WAREHOUSE=file:///tmp/iceberg-warehouse \
  -e CATALOG_URI=jdbc:sqlite:/catalog/catalog.db \
  -e CATALOG_JDBC_USER=u -e CATALOG_JDBC_PASSWORD=p \
  -e CATALOG_IO__IMPL=org.apache.iceberg.hadoop.HadoopFileIO \
  tabulario/iceberg-rest:latest
```

The k3d TiCDC pod accesses `localhost:8181` via `hostNetwork: true` on the StatefulSet (edit `local-dev/cli.py`'s manifest generator).

**Verification (either option)**:

```bash
curl -s http://localhost:8181/v1/config | head -200
# JSON config response confirming warehouse=file:///iceberg-warehouse
```

### 6.3.1 Why Tabular's iceberg-rest stands in for production catalogs

The image implements the same Apache Iceberg REST Catalog Open API spec as production catalogs. iceberg-go's REST client speaks this spec; the same client code runs against any conformant backend. Production catalogs may have additional concerns the local stack doesn't simulate:

- **Authentication**: production may need OAuth tokens, mTLS, or service-mesh-injected headers. iceberg-go provides hooks (`rest.WithToken`, `rest.WithCustomTransport`, `rest.WithHeaders`) that wire these in without modifying the rest of the sink.
- **Vended credentials**: some catalogs issue scoped per-table S3 tokens; some don't and reject the `X-Iceberg-Access-Delegation` header. iceberg-go exposes `rest.WithHeaders` to suppress problem headers.
- **Storage backend**: file:// for local; s3://, gs://, abfs:// in production. iceberg-go's FileIO is plugin-based; the sink code is FileIO-agnostic.

For v1, the local-stack target is correctness against a vanilla spec-conformant catalog with filesystem storage. Production wiring is a config-line set of changes covered by the design's adapter hooks (§13), not a code rewrite.

### 6.4 Local storage warehouse

Iceberg data files, manifests, and metadata.json all land under `/tmp/iceberg-warehouse/`. The sink writes there directly via iceberg-go's filesystem `FileIO` (no S3, no object-store wrapper). The catalog's metadata pointer also references `file://` paths.

There is no IAM, no bucket policy, no UA injection, no mesh routing. The sink's only network calls are: (a) PD-etcd (localhost:2379, already used by TiCDC), (b) the REST catalog (localhost:8181), and (c) TiCDC-internal RPC.

### 6.5 Connectivity between TiCDC pod, iceberg-rest, and warehouse

The deployment topology depends on which §6.3 option was chosen:

**With Option A (in-cluster iceberg-rest, recommended on macOS)**: TiCDC pod accesses iceberg-rest at `http://iceberg-rest.tidb-local.svc.cluster.local:8181`. Both TiCDC and iceberg-rest mount the same `iceberg-warehouse-pvc` PersistentVolumeClaim, so both see Parquet files at `/iceberg-warehouse/...`. Configure the TiCDC pod's volume mount via the TidbCluster CRD's `spec.ticdc.additionalVolumes` and `additionalVolumeMounts`.

The Mac host accesses iceberg-rest via `kubectl port-forward svc/iceberg-rest 8181:8181`, useful for the bootstrap utility (§16.1) and validation queries.

**With Option B (host-network iceberg-rest, Linux only)**: edit `local-dev/cli.py`'s TidbCluster manifest generator to set `spec.ticdc.hostNetwork: true`. The TiCDC pod then reaches `localhost:8181` on the host and `/tmp/iceberg-warehouse` via a host-path mount.

The implementation does not depend on which option was used — the sink config's `iceberg://...` URL is the only thing that changes.

### 6.6 End-to-end smoke test before implementation begins

Before writing any sink code, the agent confirms the local stack works by running the existing iceberg-go smoke test against the local REST catalog. The smoke test code lives in `iceberg-irc-smoke/` (see related design ancestor); it has 5 subcommands. Run them in order:

```bash
cd <repo-root>/iceberg-irc-smoke
go run . basic        # write a Parquet file, commit, read it back
go run . schema       # AddColumn / RenameColumn / UpdateColumn / DeleteColumn
go run . partitioned  # identity-partitioned write
go run . concurrent   # two-goroutine CAS race; one wins, one gets ErrCommitFailed
go run . handoff      # parallel-write + single-commit (the prod shape)
```

All five must pass. If any fails, the local stack is broken; fix that before implementing the sink.

The agent then has both: a working TiDB+TiCDC cluster on k3d, and proof that iceberg-go can write to the local catalog. The remaining work is to wire these together inside TiCDC.

## 7. Architecture

### 7.1 Components and where they run

```
TiDB
  ↓
TiKV ──── Log Service ──── EventCollector ──── Dispatcher (1 per span)
                                                       ↓
                                      Iceberg Sink (1 per Dispatcher)
                                                       ↓
                                      Encoder → Writer → Path Reporter
                                                                        ↘
                                                                          ↘
                              etcd Committer Lease (per Iceberg table)     ↘
                                                                            ↘
                                                                             ↓
                                       Commit Agent (1 per Iceberg table,
                                       runs only on lease holder node)
                                                                             ↓
                                       iceberg-go: AddFiles + CAS commit
                                                                             ↓
                                       Iceberg REST catalog (localhost:8181)
                                                                             ↓
                                       Local filesystem warehouse (/tmp/iceberg-warehouse)
```

### 7.2 Two roles per node

Every TiCDC node always runs the **writer role**: for each Dispatcher it hosts, encode events to Parquet and write them to the warehouse immediately. Writers operate independently, never coordinate with each other for the data path.

Every TiCDC node *may* run the **commit-agent role** for some Iceberg tables: the ones for which it currently holds the etcd lease. A typical 5-node cluster holding 200 Iceberg tables ends up with each node holding ~40 leases. Lease assignment rebalances on node join/leave/failure.

### 7.3 Three flows: data, commit, DDL

**Data flow** — continuous, microsecond-scale latency at TiCDC, no coordination:
1. Dispatcher receives DMLEvent.
2. Sink encoder converts `chunk.Row` → Arrow record.
3. Writer buffers records into a Parquet file, rotates when target size or commit-interval reached.
4. On rotation: write footer, write file to warehouse, fire each row's `PostFlush`, send `IcebergPathReport` (path + min/max CRTS + counts) to the commit agent.

**Commit flow** — per-table, every `commit-interval` seconds, single-writer CAS:
1. Commit agent timer fires (60s default + per-changefeed jitter).
2. Read current cluster `checkpoint_ts`; pick barrier `B = checkpoint_ts`.
3. From in-memory file batch, select files where `max_crts < B`. Hold any with `min_crts >= B`.
4. Call iceberg-go: `txn := tbl.NewTransaction(); txn.AddFiles(...); txn.SetSnapshotProperties({"ticdc.commit-barrier-ts": B}); txn.Commit(ctx)`.
5. On success: drop committed entries; update metrics. On `ErrCommitFailed`: refresh table state, retry (custom loop — iceberg-go's built-in retry has a known bug at v0.x).

**DDL flow** — per-DDL-event, synchronous barrier across all dispatchers writing to the table:
1. Dispatcher's `WriteBlockEvent(ddl)` arrives at sink. By contract, all DMLs with `commit_ts < ddl.commit_ts` already PostFlush-ed.
2. Sink force-rotates any open Parquet files; sends `DDLArrivedAt(table, ddl, T, dispatcher_id)` to commit agent; blocks for `DDLAppliedAck`.
3. Commit agent collects `DDLArrivedAt` from all participating dispatchers (count from Maintainer's span-to-node map). Drains files with `max_crts < T`, commits them under the current schema. Applies the DDL to Iceberg via `txn.UpdateSchema()`. Broadcasts `DDLAppliedAck(new_schema_id, outcome)`.
4. Sinks receive ack, update local schema cache, return from `WriteBlockEvent`. Dispatcher resumes DML delivery.

### 7.4 Why this topology

- **N writers + 1 committer per Iceberg table** is the industry-standard pattern (Flink-Iceberg's parallelism=1 operator, Kafka Connect-Iceberg's coordinator task, Spark Streaming's driver). Single-writer CAS is the only way to guarantee consistent commits without distributed locks.
- **Both roles always live in the TiCDC process**. No separate "committer service." Reuses TiCDC's existing etcd session, messaging, lifecycle, and the commit-agent footprint is small (one goroutine per held lease).
- **Per-Iceberg-table lease, not per-changefeed**: a single changefeed can write to multiple Iceberg tables (one per source TiDB table). Each Iceberg target gets its own lease and its own commit cadence.

### 7.5 New packages and existing-file edits

**New packages** (additions only, no existing code modified):
- `downstreamadapter/sink/iceberg/` — sink, writer, encoder, path reporter, schema cache
- `downstreamadapter/sink/iceberg/committer/` — commit agent, lease holder, DDL coordinator
- `pkg/sink/iceberg/` — config types, URL parsing

We do NOT add `pkg/sink/codec/iceberg/`. The existing codec packages (`csv`, `canal`, `avro`) implement a row-encoder protocol that returns byte buffers, which doesn't match our shape (columnar Arrow → Parquet → file). Encoding lives inside `downstreamadapter/sink/iceberg/encoder.go`. We do lift ~80 LOC of TiDB-side decoding from `pkg/sink/codec/csv/csv_message.go:fromColValToCsvVal` (the per-type `switch` over `mysql.Type*` calling `chunk.Row` accessors) — this decoding is unrelated to CSV output and works equally well as input to Arrow column builders.

**Existing-file edits** (additive only):
- `downstreamadapter/sink/sink.go:44-63` — one new `case config.IcebergScheme:` in the URL scheme dispatch
- `pkg/config/sink_uri.go` — one new URL scheme constant `IcebergScheme = "iceberg"`
- `pkg/common/types.go` — one new sink type constant
- `pkg/messaging/registry.go` — three new message types (`IcebergPathReport`, `DDLArrivedAt`, `DDLAppliedAck`)
- `pkg/etcd/etcdkey.go` — one new key prefix `iceberg-committer/`

**Total edits to existing files: ~14 lines.** Everything else is in new packages.

**Estimated total new code**: 3,500–4,500 LOC. Largest novel piece is the commit agent + DDL coordinator (~1,000 LOC). Encoder + writer is ~1,000 LOC, with ~140 LOC lifted from existing csv codec. Lease/election layer is ~300 LOC reusing `concurrency.Election`. Remainder is config, wiring, and tests.

## 8. Sink interface and registration

### 8.1 The Sink interface

TiCDC's sink interface in `downstreamadapter/sink/sink.go:31-42`:

```go
type Sink interface {
    SinkType() common.SinkType
    IsNormal() bool
    AddDMLEvent(event *commonEvent.DMLEvent)
    WriteBlockEvent(event commonEvent.BlockEvent) error
    AddCheckpointTs(ts uint64)
    SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore)
    Close(removeChangefeed bool)
    Run(ctx context.Context) error
}
```

Our sink implements this interface exactly. No interface changes.

| Method | Iceberg sink behavior |
|---|---|
| `SinkType()` | returns `common.IcebergSinkType` (new constant) |
| `IsNormal()` | atomic boolean; flipped to false on unrecoverable errors |
| `AddDMLEvent(e)` | hand off to local Encoder → Writer (§9, §10); register `e.PostFlush` callback to fire when the Parquet file containing this row is closed and written |
| `WriteBlockEvent(e)` | DDL: run the 3-phase DDL protocol (§14); blocks until `DDLAppliedAck` received |
| `AddCheckpointTs(ts)` | inform local commit agent (if this node holds any leases for this changefeed's targets) that cluster checkpoint advanced |
| `SetTableSchemaStore(s)` | store reference; used by encoder to look up TableInfo |
| `Close(removeChangefeed)` | stop accepting events; flush in-flight Parquet files; if removeChangefeed, release held leases (don't wait for TTL); release etcd session |
| `Run(ctx)` | start writer goroutines, commit-agent goroutines for held leases, lease-watcher goroutines; cancel on ctx.Done |

### 8.2 The dispatch switch entry

In `downstreamadapter/sink/sink.go`'s `New()` switch, add:

```go
case config.IcebergScheme:
    return iceberg.New(ctx, changefeedID, sinkURI, cfg.SinkConfig, cfg.EnableTableAcrossNodes)
```

Mirror in `Verify()`.

### 8.3 URL scheme

```
iceberg://<catalog-host>:<port>/?<options>
```

Local example:
```
iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse&database-prefix=tidb_
```

The `warehouse=` query parameter selects the catalog warehouse. The `database-prefix=` parameter prepends to source-DB names when computing Iceberg target names.

### 8.4 Configuration

`pkg/sink/iceberg/config.go` exports:

```go
type Config struct {
    // Cadence
    CommitInterval     time.Duration `toml:"commit-interval"`     // default 60s
    CommitIntervalMin  time.Duration `toml:"commit-interval-min"` // default 15s

    // File sizing
    TargetFileSize         int64 `toml:"target-file-size"`         // default 64 MiB
    ParquetRowGroupSize    int64 `toml:"parquet-row-group-size"`   // default 128 MiB
    ParquetCompression     string `toml:"parquet-compression"`     // default "zstd"

    // Layout
    PartitionBy           string `toml:"partition-by"`             // "hour" (default) or "day"
    FileNameFormat        string `toml:"file-name-format"`         // template; see §10.4

    // Naming
    IcebergDatabasePrefix string `toml:"iceberg-database-prefix"`  // empty by default
    IcebergTableSuffix    string `toml:"iceberg-table-suffix"`     // "_cdc" by default

    // Manifest behavior
    ManifestMergeThreshold int `toml:"manifest-merge-threshold"`   // default 8 (Iceberg's own default)
}
```

No catalog-host-header, no UA, no vended-credentials toggles. Pure REST + filesystem.

## 9. Encoder: TiDB row → Arrow record → Parquet

### 9.1 What the encoder receives

Per `pkg/common/event/dml_event.go`, every `Sink.AddDMLEvent(e *DMLEvent)` delivers one TiDB transaction's worth of row changes for one table-span. The encoder iterates with `e.GetNextRow()` to consume:

```go
type RowChange struct {
    PreRow   chunk.Row              // populated for Update + Delete
    Row      chunk.Row              // populated for Insert + Update
    RowType  common.RowType         // Insert | Update | Delete
    Checksum *integrity.Checksum    // optional
    RowKey   []byte                 // encoded primary-key bytes
}
```

Plus the event itself carries `e.CommitTs`, `e.StartTs`, `e.Seq`, `e.PhysicalTableID`, `e.TableInfo`.

### 9.2 The Iceberg target schema

For a source TiDB table `<db>.<table>` with columns `c1 ... cN`, the Iceberg CDC log table:

```
struct {
  _op            string             ("I" | "U" | "D")
  _commit_ts     long               raw TSO (uint64 stored as int64)
  _commit_dt     timestamp(micro)   derived from _commit_ts
  _start_ts      long               raw TSO
  _seq           long               TiCDC per-event sequence
  _table_id      long               physical table ID
  data           struct<c1: T1, ..., cN: TN>?   post-image, NULL for Delete
  old            struct<c1: T1, ..., cN: TN>?   pre-image, NULL for Insert
  dt             string             "YYYY-MM-DD" partition column
  hr             string             "HH" partition column
}
```

Iceberg partition spec: `identity(dt)` (field 1000), `identity(hr)` (field 1001).

### 9.3 Type mapping (TiDB → Arrow → Iceberg)

v1 has no per-column override. The mapping below applies uniformly. Any source column the policy can't unambiguously place causes the changefeed to fail at create time, not at runtime.

| TiDB type | Arrow type | Iceberg type | Notes |
|---|---|---|---|
| TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT (signed) | int64 | LongType | Always int64 — widest type day 1. |
| BIGINT UNSIGNED | string | StringType | uint64 doesn't fit in int64. Stringify to avoid overflow. |
| FLOAT | float64 | DoubleType | Promote to double. |
| DOUBLE | float64 | DoubleType | |
| DECIMAL(p, s) | decimal128(p, s) | DecimalType(p, s) | Iceberg supports up to (38,38); larger fails at create time. |
| CHAR, VARCHAR, TEXT, MEDIUMTEXT, LONGTEXT, TINYTEXT | string (UTF-8) | StringType | |
| BINARY, VARBINARY, BLOB, MEDIUMBLOB, LONGBLOB, TINYBLOB | binary | BinaryType | No base64 — Parquet stores bytes natively. |
| DATE | date32 | DateType | |
| TIME | int64 (microseconds since midnight) | TimeType | |
| TIMESTAMP | timestamp(micro, UTC) | TimestampType (with-tz) | TiDB TIMESTAMP is TZ-aware; emit as UTC. |
| DATETIME | timestamp(micro) | TimestampType (without-tz) | TiDB DATETIME is TZ-naive. |
| YEAR | int32 | IntegerType | |
| ENUM | string | StringType | Convert numeric to text label via TableInfo enum definitions. |
| SET | string | StringType | Comma-separated text. |
| JSON | string | StringType | Stored as JSON text (TiDB's canonical serialization). |
| BIT(n) where n ≤ 64 | int64 | LongType | Pack into int64. |
| BIT(n) where n > 64 | binary | BinaryType | Big-endian bytes. |

Three columns where v1 fails loudly at changefeed create:
- BIGINT UNSIGNED — supported as string, but customers should know.
- DECIMAL(p, s) where p > 38.
- Geometry / GIS types — out of scope.

### 9.4 Encoding rules per RowType

| TiDB op | `_op` | `data` | `old` |
|---|---|---|---|
| Insert | "I" | from `RowChange.Row` | NULL |
| Update | "U" | from `RowChange.Row` (post) | from `RowChange.PreRow` (pre) |
| Delete | "D" | NULL | from `RowChange.PreRow` |

Cardinality: one Iceberg row per RowChange. A TiDB transaction with 5 row changes produces 5 Iceberg rows sharing the same `_commit_ts` and `_start_ts`.

### 9.5 NULL and missing columns

- **Column value is SQL NULL**: Arrow null marker; round-trips cleanly.
- **Column is absent from `TableInfo` but present in Iceberg schema** (e.g. dropped via DDL but the absorb policy left it in Iceberg): write null, log once per restart.
- **Column is in `TableInfo` but not yet in Iceberg schema**: blocked by the DDL barrier protocol (§14); should not occur at runtime.

### 9.6 Schema cache

Per-sink cache keyed on `event.TableInfoVersion`:

```go
type schemaCache struct {
    mu      sync.RWMutex
    current struct {
        tableInfoVersion uint64
        icebergSchema    *iceberg.Schema
        arrowSchema      *arrow.Schema       // built once per Iceberg schema, reused per row
        columnMapping    []columnMap          // index: TiDB column ordinal → Iceberg field ID + Arrow type
    }
}
```

Refreshed on: DDL apply ack (committer notifies via `DDLAppliedAck.NewSchemaID`); sink Run startup (`cat.LoadTable`); defensive reload if encoder sees a `TableInfoVersion` newer than cached.

### 9.7 Reuse from existing TiCDC codecs

Lifted from `pkg/sink/codec/csv/csv_message.go`:

| Logic | Source | LOC | Adaptation |
|---|---|---|---|
| Per-type switch over `mysql.Type*` | `fromColValToCsvVal` lines 277-333 | ~80 | Keep the switch and accessor calls. Replace return type from `any` (CSV-bound) with calls into Arrow column builders. |
| ENUM/SET → text label | lines 304-317 | ~14 | `types.ParseEnumValue(colInfo.GetElems(), …)` and `ParseSetValue(…)` — exactly what we want. |
| BIT(n) → integer | lines 318-321 | ~3 | For n ≤ 64; for n > 64 we feed bytes to a binary builder. |
| Skip-virtual-generated-column | line 393 | ~3 | `if col.IsVirtualGenerated() continue`. |
| RowChange iteration with I/U/D routing | `csv_encoder.go:46-65` | ~40 | The pattern of `event.GetNextRow()` loop + dispatch per RowType. |

Total lift: ~140 LOC. The novel work is the Arrow column builders, schema-cache logic, and TableInfo→Arrow→Iceberg schema translation.

### 9.8 Out of scope for v1

- Per-column type overrides — v2 concern.
- Sanitization (column nulling for PII) — TiCDC's existing `ColumnSelector` (`downstreamadapter/sink/columnselector/`) handles this at the Dispatcher layer; we rely on it.
- Custom synthetic columns beyond the listed set.

## 10. Writer: Parquet rotation, file naming, warehouse write

### 10.1 One writer per Dispatcher

A TiCDC node hosts N Dispatchers; each Dispatcher's `Sink.AddDMLEvent` lands in its own writer goroutine. Writers don't share Arrow buffers or any other state.

### 10.2 File rotation policy

Rotate when ANY of:

1. **Size cap**: cumulative encoded bytes ≥ `target-file-size` (default 64 MiB).
2. **Row group cap**: file's row count exceeds `parquet-row-group-size / avg row size`.
3. **Time cap**: file open for ≥ `commit-interval`.
4. **DDL barrier**: `WriteBlockEvent(ddl)` arrives.
5. **Partition boundary**: next event's `(dt, hr)` differs from current file's. Each Parquet file holds rows from exactly one partition.
6. **Sink shutdown**: graceful close drains all open files.

After rotation:
- Write the Parquet footer.
- Write the file to the warehouse via iceberg-go's FileIO (file:// for local; later s3:// or other).
- Fire the row-level `PostFlush` callbacks for every row in this file.
- Send `IcebergPathReport(path, span_id, dispatcher_id, min_crts, max_crts, row_count, byte_count)` to the commit agent.

### 10.3 PostFlush wiring (the durability handshake)

The contract:

> A row's `PostFlush` callback fires only after the Parquet file containing that row is closed AND written to the warehouse successfully.

If the warehouse write fails:
- Retry with exponential backoff (10ms → 100ms → 1s → 10s, max 5 retries).
- On final failure: `IsNormal()=false`, return error from the writer goroutine. Dispatcher sees failure on next `AddDMLEvent` or via the changefeed error-status channel.

This means a slow warehouse holds back the dispatcher's `checkpointTs`, which holds back the cluster checkpoint — the correct backpressure path.

### 10.4 File path layout

```
<warehouse-root>/<iceberg-db>/<iceberg-table>/data/dt=<YYYY-MM-DD>/hr=<HH>/ticdc-<dispatcher_id>-<seq>-<uuid>.parquet
```

- `<warehouse-root>` is whatever the catalog reports as the table location.
- `dispatcher_id` (UUID-shaped) ensures no cross-writer name collision in split-table mode.
- `seq` is per-dispatcher monotonic, reset on dispatcher restart. The `uuid` suffix handles restart-replay collision.
- File-name format is configurable but we strongly recommend the default — Iceberg's tooling around file recovery, snapshot expiration, and orphan-file cleanup all assume similar shapes.

### 10.5 Crash recovery

If the writer goroutine crashes:
- In-memory Arrow buffers are lost.
- Already-written Parquet files are durable in the warehouse.
- Path-reports already sent to the commit agent are durable in the agent's in-memory batch (and in Iceberg metadata after next commit).
- On restart, the dispatcher replays from the last-acked checkpoint. Already-written-but-not-yet-committed Parquet files become orphans; standard Iceberg `remove_orphan_files` table maintenance handles cleanup.

### 10.6 What we delegate to iceberg-go

iceberg-go provides:
- The `pqarrow.WriteTable(arrowTable, parquetWriter, props)` path for Arrow → Parquet.
- The `tbl.FS(ctx)` interface for warehouse access (returns a `WriteFileIO` we call `.Create(path).Write(buf).Close()` on).
- File-stats extraction — when we call `txn.AddFiles(ctx, paths)`, iceberg-go reads each Parquet footer to extract per-column min/max/null-count for manifest building.

~150 LOC of writer code total.

## 11. Commit agent, lease, and barrier picking

### 11.1 Lease lifecycle

We reuse `concurrency.Election` from etcd's `client/v3/concurrency`, the same library TiCDC uses for Coordinator and Log Coordinator election (`server/module_election.go:campaignCoordinator`).

Per Iceberg target table T, on every TiCDC node hosting a Dispatcher writing to T, a per-table goroutine runs:

```go
session := /* existing TiCDC etcd session */
key := fmt.Sprintf("/tidb/cdc/%s/__cdc_meta__/iceberg-committer/%s", clusterID, T.fqn())
election := concurrency.NewElection(session, key)
nodeID := server.info.ID

for ctx.Err() == nil {
    if err := election.Campaign(ctx, nodeID); err != nil {
        // Backoff per existing TiCDC pattern.
        continue
    }
    runCommitAgent(ctx, T)
    // Returns on graceful shutdown OR session loss. Either way, lease released.
}
```

Lease TTL = whatever the TiCDC etcd session uses (single-digit seconds in practice).

### 11.2 Discovering the current leaseholder (writer side)

Writers use `election.Observe(ctx)` to get a stream of leaseholder updates. Each writer maintains a local `leaseholderCache: map[IcebergFQN]NodeID` keyed by Iceberg table identifier. Writers consult this cache before sending each cross-node message.

### 11.3 In-memory file batch state

Per held lease:

```go
type fileBatch struct {
    icebergIdent  catalog.Identifier
    pending       []FileEntry
    schemaVersion int
    lastCommitB   uint64
}

type FileEntry struct {
    path         string
    spanID       string
    dispatcherID common.DispatcherID
    minCrts      uint64
    maxCrts      uint64
    rowCount     int64
    byteCount    int64
    receivedAt   time.Time
}
```

Memory pressure is bounded: at 60s commit cadence, the agent never holds more than ~1 minute's worth of file paths per table — typically dozens to low hundreds of FileEntries per table.

### 11.4 The commit cycle

Triggered every `commit-interval` seconds plus per-changefeed jitter (`hash(changefeed_id) % 1000ms`):

```
read clusterCheckpointTs from local Maintainer
B = clusterCheckpointTs

select files from `pending` where maxCrts < B
if empty: skip this cycle

tbl, err := catalog.LoadTable(ctx, ident)        // refresh
txn := tbl.NewTransaction()
err = txn.AddFiles(ctx, paths, nil, false)
txn.SetSnapshotProperties({
    "ticdc.commit-barrier-ts": fmt.Sprint(B),
    "ticdc.changefeed-id":     changefeedID.Name(),
    "ticdc.cluster-id":         clusterID,
})
newTbl, err := txn.Commit(ctx)

if err:
    if errors.Is(err, icetable.ErrCommitFailed):
        // CAS conflict — rare since lease guarantees single-writer.
        // Possible causes: stale snapshot read, lease changeover during commit, external manual edit.
        // Custom retry with refresh; iceberg-go's built-in retry has a known bug.
        sleep(jittered backoff up to 1s)
        retry
    else:
        log fatal, IsNormal=false

remove committed FileEntries from `pending`
update lastCommitB = B
record metrics
```

### 11.5 Why barrier B = clusterCheckpointTs

At any moment, the cluster's externally-reported `checkpoint_ts = C` guarantees:
- Every row with `commit_ts ≤ C` is durably written to the sink (PostFlush fired, Parquet file written).
- No row with `commit_ts ≤ C` will ever appear from the cluster again.

Setting `B = C` is correct AND maximally fresh. No safety margin needed.

### 11.6 Snapshot properties

Recorded on every commit:
- `ticdc.commit-barrier-ts` — TSO (uint64 stored as decimal string).
- `ticdc.changefeed-id`, `ticdc.cluster-id` — audit trail.

Iceberg's standard summary fields (`added-data-files`, `added-records`, etc.) are computed by iceberg-go automatically.

Consumers convert TSO to wall-clock via `physical_ms = ts >> 18` (TiDB's TSO formula).

### 11.7 Lease changeover

Three failure modes:

**(a) Old leaseholder's node dies.** Etcd session expires (TTL bound), key disappears. New campaign winner takes over. Writers see new leaseholder via `Observe` watch, route subsequent path-reports correctly. Files written by the dead node are durable; their FileEntries arrived in messages but are now lost. **Recovery**: see §11.8.

**(b) Voluntary resignation** (graceful shutdown). Try to commit pending files first; if commit fails, log and resign anyway.

**(c) Network partition.** Etcd's Raft majority resolves; minority drops session. Majority side keeps or re-elects the leader. Minority's commit attempts fail at Iceberg CAS (its view is stale).

### 11.8 Reconciliation: finding orphan files at takeover (warehouse LIST)

When a new committer takes over for table T, files may exist in the warehouse but not yet in any Iceberg snapshot. We use a warehouse-scan approach:

```
last_snapshot = tbl.CurrentSnapshot()
last_barrier = parse(last_snapshot.summary['ticdc.commit-barrier-ts'])

// LIST the warehouse data prefix for this table
list <warehouse>/<db>/<table>/data/

// For each file:
//   read parquet footer for stats (or parse from filename)
//   if file is not referenced in last_snapshot's manifests AND max_crts > last_barrier:
//      add to pending batch with synthesized FileEntry
```

This uses iceberg-go's filesystem `FileIO.List(prefix)` for local; the same code works against object stores by changing FileIO.

The cost is paid only on lease changeover. Worst case: a few seconds of warehouse LIST + footer reads before first commit. Acceptable.

### 11.9 Goroutine layout

Per node:
- 1 lease-watcher goroutine per Iceberg target the local Dispatchers write to (cheap; mostly blocked on `Observe`)
- 1 commit-agent goroutine per Iceberg target the node currently leads

Scale at 1,000 tables × 5 nodes: ~5,000 watcher goroutines cluster-wide, ~200 commit agents per node. Trivial for Go's runtime.

### 11.10 New code

| Piece | LOC | Notes |
|---|---|---|
| Lease loop with `Election.Campaign` | ~80 | Pattern from `module_election.go:campaignCoordinator` |
| `Observe` watcher + leaseholder cache | ~60 | new |
| In-memory `fileBatch` + locking | ~80 | new |
| Commit cycle (load → AddFiles → SetProperties → Commit → custom CAS retry) | ~150 | smoke test `handoff.go` is the working reference |
| Reconciliation warehouse scan | ~120 | new |
| Path-report message handler | ~60 | new |
| Tests | ~400 | mirrors smoke test patterns |

**~550 LOC + ~400 tests.**

## 12. Iceberg table layout

### 12.1 Source-to-Iceberg name mapping

A TiDB source `<source-db>.<source-table>` maps to Iceberg `<iceberg-db>.<iceberg-table>` via:

```toml
[sink.iceberg]
iceberg-database-prefix = ""        # source TiDB db prefixed with this; default empty
iceberg-table-suffix    = "_cdc"    # source table suffixed with this; default
```

Default produces: `mydb.users → mydb.users_cdc`. Operators pre-create the Iceberg target before pointing the changefeed at it.

### 12.2 Warehouse selection

Single warehouse per changefeed, configured via the URL:

```
iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse
```

The catalog records the warehouse internally; iceberg-go uses it implicitly when creating tables. Our sink never constructs warehouse paths directly — it asks the catalog where to put files.

### 12.3 Partition spec

```
PartitionSpec:
  field 1000: identity(dt)   // string column "YYYY-MM-DD"
  field 1001: identity(hr)   // string column "HH"
```

Derived in the encoder from `_commit_ts`:

```
physical_ms = ts >> 18
utc_time    = time.UnixMilli(physical_ms).UTC()
dt          = utc_time.Format("2006-01-02")
hr          = utc_time.Format("15")
```

### 12.4 Snapshot properties (recap)

Set by the commit agent on every commit:

| Key | Value |
|---|---|
| `ticdc.commit-barrier-ts` | TSO (decimal string of uint64) |
| `ticdc.changefeed-id` | changefeed display name |
| `ticdc.cluster-id` | TiCDC cluster ID |

### 12.5 Table-property defaults at bootstrap

Set on `cat.CreateTable()`:

```
write.parquet.compression-codec       = zstd
write.parquet.row-group-size-bytes    = 134217728   # 128 MiB
write.target-file-size-bytes          = 67108864    # 64 MiB
write.metadata.compression-codec      = gzip
write.distribution-mode               = none
format-version                         = 2
```

## 13. Catalog adapter and storage I/O

### 13.1 Catalog construction

iceberg-go's REST catalog client:

```go
cat, err := rest.NewCatalog(ctx, "ticdc-iceberg", catalogURI)
if err != nil {
    return nil, errors.Trace(err)
}
```

For local development, `catalogURI` is `http://localhost:8181/`. No mesh routing, no host-header rewriting, no header stripping, no UA injection. v1 supports only catalogs that work out of the box with vanilla iceberg-go.

If the catalog needs auth (token, OAuth), pass options:

```go
cat, err := rest.NewCatalog(ctx, "ticdc-iceberg", catalogURI,
    rest.WithToken(cfg.CatalogToken),  // if configured
)
```

### 13.2 Storage I/O

iceberg-go's `tbl.FS(ctx)` returns a `FileIO` implementation chosen by the table's location prefix:

- `file://...` → local filesystem
- `s3://...` → S3
- (others: gs://, abfs://, etc., via iceberg-go FileIO plugins)

For local development, the warehouse is `file:///tmp/iceberg-warehouse`, so all writes go to local disk. No S3 client, no IAM, no UA injection.

Cloud deployment is a configuration change (catalog returns `s3://` paths instead of `file://`), not a code change. Out of scope for v1.

### 13.3 Authentication

For local development, none.

For cloud deployment (out of scope for v1):
- REST catalog: token via `rest.WithToken(...)` or OAuth flow.
- S3 storage: ambient AWS credentials (IRSA, EC2 instance role, env vars). iceberg-go uses the AWS SDK's default credential chain.

## 14. DDL protocol — detailed

### 14.1 Roles and message types

| Role | Where | What it does for DDL |
|---|---|---|
| Sink (writer side) | Per Dispatcher | Receives `WriteBlockEvent(ddl)`; flushes local Parquet; sends `DDLArrivedAt`; blocks on `DDLAppliedAck`. |
| Commit Agent | Per Iceberg table, on lease holder | Collects `DDLArrivedAt` from all participating dispatchers; commits pre-DDL files; applies DDL to Iceberg; broadcasts `DDLAppliedAck`. |
| Maintainer (existing) | Per changefeed | Provides "which dispatchers contribute to this Iceberg table" via existing span-to-node map. Read only. |

Three new message types in `pkg/messaging/`:

```go
type DDLArrivedAt struct {
    IcebergFQN     string
    DDLPayload     []byte           // serialized DDLEvent
    CommitTs       uint64           // T
    DispatcherID   common.DispatcherID
    SourceTableID  int64
}

type DDLAppliedAck struct {
    IcebergFQN     string
    CommitTs       uint64
    NewSchemaID    int
    Outcome        DDLOutcome       // Applied | Absorbed | Rejected
    Error          string
}

type DDLRejected struct {
    IcebergFQN     string
    CommitTs       uint64
    Reason         string
}
```

### 14.2 Phase 1 — writer reaches barrier

```go
func (s *icebergSink) WriteBlockEvent(e BlockEvent) error {
    ddl := e.(*DDLEvent)

    // 1. Force-rotate any open Parquet files.
    if err := s.writer.RotateAll(); err != nil {
        return err
    }

    // 2. Find the commit agent for this table.
    target := s.icebergTargetFor(ddl.SchemaName, ddl.TableName)
    leaseholder := s.leaseholderCache.Load(target)

    // 3. Send DDLArrivedAt; block waiting for ack.
    ack, err := s.messaging.RequestReply(ctx,
        leaseholder,
        &DDLArrivedAt{
            IcebergFQN:    target.FQN(),
            DDLPayload:    ddl.Marshal(),
            CommitTs:      ddl.CommitTs,
            DispatcherID:  s.dispatcherID,
            SourceTableID: ddl.TableID,
        },
        ddlAckTimeout,
    )
    if err != nil {
        return err
    }

    // 4. Apply ack outcome.
    switch ack.Outcome {
    case DDLOutcomeApplied, DDLOutcomeAbsorbed:
        s.schemaCache.Update(ack.NewSchemaID)
        return nil
    case DDLOutcomeRejected:
        s.isNormal.Store(false)
        return errors.New("DDL rejected: " + ack.Error)
    }
}
```

### 14.3 Phase 2 — commit agent collects, drains, applies

Per-table state:

```go
type ddlState struct {
    commitTs       uint64
    payload        *DDLEvent
    arrivals       map[common.DispatcherID]bool
    expectedCount  int
    deadline       time.Time
}
```

On `DDLArrivedAt`:

```
if no in-flight ddlState OR existing.commitTs != msg.CommitTs:
    create new ddlState{commitTs, payload, arrivals: {dispatcherID: true},
                       expectedCount: maintainer.ExpectedDispatcherCount(target, msg.CommitTs),
                       deadline: now + ddlPhase2Timeout}
else:
    state.arrivals[msg.DispatcherID] = true

if len(state.arrivals) == state.expectedCount:
    proceed to drain-and-apply
```

Drain-and-apply:

```
1. Drain pending file batch:
   - Files with maxCrts < T → eligible for pre-DDL commit
   - Files with minCrts >= T → hold for next commit (post-DDL schema)
   - (No straddling — see §7.3)

2. Pre-DDL commit:
   if any eligible files:
     tbl, _ := catalog.LoadTable(ctx, ident)
     txn := tbl.NewTransaction()
     txn.AddFiles(ctx, paths, ...)
     txn.SetSnapshotProperties({"ticdc.commit-barrier-ts": T-1, ...})
     newTbl, err := txn.Commit(ctx)
     // standard CAS retry on ErrCommitFailed

3. Apply DDL to Iceberg:
   match payload.GetDDLType():
     case ActionAddColumn:
       us.AddColumn([]string{colName}, mappedIcebergType, doc, false, nil)
       outcome = Applied
     case ActionRenameColumn:
       us.RenameColumn([]string{old}, new)
       outcome = Applied
     case ActionDropColumn:
       outcome = Absorbed              // log only
     case ActionModifyColumn:
       if isTypePromote(payload):  outcome = Absorbed   // already widest
       elif isTypeNarrow(payload): outcome = Rejected
       else:                       outcome = Absorbed
     case ActionTruncateTable, ActionCreateTable, ActionDropTable:
       outcome = Rejected              // §5
     default:
       outcome = Absorbed              // CREATE INDEX etc.

4. Broadcast DDLAppliedAck to all dispatchers in state.arrivals.
```

### 14.4 Phase 3 — writers resume

On `DDLAppliedAck`:
- Update local schema cache to the new Iceberg schema.
- For Absorbed: schema cache unchanged.
- For Rejected: `IsNormal()=false`, return error.
- For Applied: rebuild the Arrow column-mapping (TiDB column ordinal → Iceberg field ID).

Return from `WriteBlockEvent`. Dispatcher resumes DML delivery.

### 14.5 DDL outcome table (consolidated)

| TiDB DDL | Outcome | Iceberg action |
|---|---|---|
| ADD COLUMN | Applied | `UpdateSchema.AddColumn` |
| RENAME COLUMN | Applied | `UpdateSchema.RenameColumn` (preserves field ID) |
| DROP COLUMN | Absorbed | log only |
| MODIFY COLUMN (type promote — narrower → widest) | Absorbed | already widest day 1 |
| MODIFY COLUMN (type narrow) | Rejected | sink fails |
| MODIFY COLUMN (other, e.g. nullability) | Absorbed for v1 | log only |
| RENAME TABLE | Conditional Apply | requires pre-created new Iceberg target; otherwise Rejected |
| TRUNCATE TABLE | Rejected | out of scope |
| CREATE TABLE | Rejected | out of scope |
| DROP TABLE | Rejected | out of scope |
| CREATE INDEX, DROP INDEX | Absorbed | TiDB-only |
| EXCHANGE PARTITION | Absorbed for v1 | partitioned tables defer |

### 14.6 Timeouts

- **`ddlAckTimeout` (writer side, default 5 min)**: how long Phase 1 waits for the ack. Exceeded → sink fails.
- **`ddlPhase2Timeout` (commit agent side, default 5 min)**: how long Phase 2 waits for `DDLArrivedAt` from all expected dispatchers. Exceeded → broadcast `DDLRejected` to those that arrived; all sinks fail loudly.

### 14.7 Lease changeover during DDL

- Commit agent dies in Phase 2: new leaseholder takes over with empty ddlState. Writers' `ddlAckTimeout` fires, they retry by re-sending to the new leaseholder. New leader collects from scratch.
- Commit agent dies after Iceberg DDL succeeded but before broadcasting acks: new leaseholder reads Iceberg, sees post-DDL schema. On (resent) `DDLArrivedAt`, replies `DDLAppliedAck` immediately — idempotent.

Schema-cache updates and ack broadcasts are idempotent.

### 14.8 New code

| Piece | LOC |
|---|---|
| Phase 1 writer-side flow | ~120 |
| Phase 2 commit-agent collection + drain + apply | ~200 |
| DDL outcome dispatch + per-DDL-type logic | ~150 |
| Message types + `pkg/messaging/` registration | ~80 |
| Tests (incl. timeout, lease changeover, idempotency) | ~500 |

**~550 LOC + ~500 tests.**

## 15. Failure modes and recovery

### 15.1 What can fail

| Component | Failure mode | Recovery mechanism |
|---|---|---|
| TiKV/PD | Existing TiCDC handles | Inherited; no Iceberg-side action |
| TiCDC node hosting writer | Process death, OOM, partition | Maintainer reschedules dispatcher; replays from last-acked checkpoint; orphan files cleaned via Iceberg `remove_orphan_files` |
| TiCDC node hosting commit agent | Same | Etcd lease expires; new node campaigns; takes over via §11.8 reconciliation |
| Warehouse write (transient) | Network blip, fs full → recover | Exponential backoff, 5 retries |
| Warehouse write (persistent) | fs read-only, permission error | Sink fails; changefeed error state |
| Catalog (transient) | Network, slow REST server | HTTP client retry on idempotent ops; CAS retry on commits |
| Catalog (persistent) | Misconfiguration, schema rejection | Sink fails |
| etcd / PD outage | Lease cannot be acquired or renewed | Existing TiCDC behavior: cluster halts. Sink halts with it. |
| DDL ack timeout | Commit agent unresponsive | Sink fails after `ddlAckTimeout`; operator investigates |
| Iceberg CAS conflict | Lease changeover mid-commit, external manual edit | Custom retry-with-refresh; up to 5 retries; then surface |

### 15.2 Three correctness invariants we never violate

1. **No row with `commit_ts ≤ ticdc.commit-barrier-ts` is missing from any committed snapshot.** Guaranteed by §10.3's PostFlush wiring + §7.3's checkpoint-TS chain.
2. **No row appears more than once in the union of committed snapshots.** Guaranteed by §11 single-writer lease + §11.8 reconciliation handling orphans correctly.
3. **No snapshot mixes pre-DDL and post-DDL rows.** Guaranteed by §14's 3-phase protocol + §10's partition rotation.

Tests in each layer assert these invariants.

### 15.3 What we do NOT recover from automatically

- TRUNCATE / CREATE TABLE / DROP TABLE delivery → halt (§5 by design).
- Type-narrowing MODIFY COLUMN → halt (§14.5 by design).
- Schema corruption (Iceberg schema diverged from TableInfo unexpectedly) → halt; operator investigates.
- Iceberg CAS exhausted (5 retries failed) → halt.

**Silent recovery from anything we don't fully understand is worse than halting loudly.**

## 16. Operations

### 16.1 Bootstrap utility

A standalone CLI (`cmd/iceberg-bootstrap`) that operators run before creating an Iceberg-sink changefeed. Its job: pre-create Iceberg tables with schemas matching the source TiDB tables.

```
iceberg-bootstrap create-table \
  --catalog http://localhost:8181/ \
  --warehouse file:///tmp/iceberg-warehouse \
  --source-tidb mydb.users \
  --iceberg-table mydb.users_cdc \
  --tidb-host 127.0.0.1 --tidb-port 4000 \
  --partition hour
```

What it does:
1. Connect to TiDB, fetch `INFORMATION_SCHEMA.COLUMNS` for the source table.
2. Apply our type-mapping table (§9.3) to derive the Iceberg schema (with `data` and `old` nested struct fields).
3. Connect to the catalog. Call iceberg-go `cat.CreateTable(ctx, ident, schema, withPartitionSpec, withProperties)`.
4. Verify creation by reading back.

~300 LOC, mostly wraps iceberg-go's table creation.

### 16.2 Validation

```
iceberg-bootstrap validate --changefeed-config changefeed.toml
```

Walks the source-to-Iceberg mapping, checks every source table's Iceberg target exists with compatible schema, surfaces any mismatches before the changefeed is started.

### 16.3 Monitoring metrics

New Prometheus metrics:

**Per-changefeed**:
- `ticdc_iceberg_files_written_total` — counter, labeled by table
- `ticdc_iceberg_bytes_written_total` — counter, labeled by table
- `ticdc_iceberg_commit_count_total` — counter
- `ticdc_iceberg_commit_duration_seconds` — histogram
- `ticdc_iceberg_commit_barrier_ts` — gauge, the most-recently-committed barrier
- `ticdc_iceberg_commit_lag_seconds` — gauge, `(now - oldest_pending_file.received_at)`

**Per-node**:
- `ticdc_iceberg_lease_held` — gauge, count of leases this node holds
- `ticdc_iceberg_pending_files` — gauge, count of in-memory FileEntries across all held leases
- `ticdc_iceberg_warehouse_write_failures_total` — counter, labeled by reason
- `ticdc_iceberg_catalog_call_duration_seconds` — histogram, labeled by call type

**DDL**:
- `ticdc_iceberg_ddl_events_total` — counter, labeled by outcome
- `ticdc_iceberg_ddl_phase2_duration_seconds` — histogram

## 17. Implementation milestones (for an autonomous coding agent)

Each milestone has an acceptance test the agent runs to declare it done. Do not advance past a milestone until its acceptance test passes.

### M1 — Scheme registration + skeleton

**Build**:
- New empty packages `downstreamadapter/sink/iceberg/`, `pkg/sink/iceberg/`.
- A skeleton `Sink` struct that implements the interface but does nothing useful (Run blocks; AddDMLEvent ignores; WriteBlockEvent returns nil; etc.).
- Add `case config.IcebergScheme: return iceberg.New(...)` in `downstreamadapter/sink/sink.go`.
- Add `IcebergScheme = "iceberg"` constant in `pkg/config/sink_uri.go`.
- Add `IcebergSinkType` constant in `pkg/common/types.go`.

**Acceptance**:
- `make cdc` succeeds.
- Manually create a changefeed with `--sink-uri="iceberg://localhost:8181/?warehouse=file:///tmp/iceberg-warehouse"` against the local k3d cluster. Verify the changefeed enters Running state without error.

### M2 — Encoder + writer producing Parquet on disk

**Build**:
- §9 encoder: TableInfo → Arrow schema; chunk.Row → Arrow record per type.
- §10 writer: file rotation (size + time + partition + DDL); write to filesystem via iceberg-go FileIO.
- §10.3 PostFlush wiring: fire row callbacks only after file is durably written.

**Acceptance**:
- Create a TiDB table, do INSERT INTO with 1000 rows.
- Manually pre-create the Iceberg table via `iceberg-bootstrap`.
- Run the changefeed (still without committer logic).
- Inspect `/tmp/iceberg-warehouse/<db>/<table>_cdc/data/dt=*/hr=*/ticdc-*.parquet` files exist and contain valid Parquet with the expected schema.
- `parquet-cli cat <file>` shows the rows correctly.

### M3 — Single-node committer

**Build**:
- §11.3 `fileBatch`, §11.4 commit cycle (no lease, no cross-node messaging — just a per-table goroutine commits files this node wrote).
- iceberg-go `txn.AddFiles + txn.Commit`.
- `ticdc.commit-barrier-ts` snapshot property.
- §11.4's custom CAS retry loop.

**Acceptance**:
- Run M2's setup but with this milestone's committer. After ~60s, an Iceberg snapshot should appear with `ticdc.commit-barrier-ts` set.
- Query the Iceberg table via `iceberg-go` CLI or a Python `pyiceberg` client; row count matches the inserted 1000 rows.
- Verify `summary['ticdc.commit-barrier-ts']` is a TSO ≥ the upstream commit_ts of all rows.
- Stop and restart the TiCDC pod; resume after restart; insert another 1000 rows; verify all 2000 rows present in Iceberg, no duplicates, no missing rows.

### M4 — etcd lease + multi-node coordination

**Build**:
- §11.1 `concurrency.Election` reused per Iceberg table.
- §11.2 `Observe`-based leaseholder cache.
- New message types `IcebergPathReport` registered in `pkg/messaging/`.
- §11.8 warehouse-LIST reconciliation at lease takeover.

**Acceptance**:
- Use `local-dev/cli.py` to bring up a 3-node TiCDC k3d cluster (manually edit cli.py to lift the TiCDC=1 cap if needed).
- Create a changefeed with `enable-table-across-nodes=true`. Insert enough rows that TiCDC splits the table across nodes.
- Verify only one node holds the lease at any time (check etcd via `etcdctl get --prefix /tidb/cdc/.../iceberg-committer/`).
- Kill the leader pod. Verify a new leader takes over within 15s, runs warehouse-LIST reconciliation, and the next commit includes any orphan files.
- Final row count in Iceberg matches inserted count exactly.

### M5 — DDL protocol

**Build**:
- §14 three-phase DDL coordination.
- DDL outcome dispatch per §14.5.
- Message types `DDLArrivedAt`, `DDLAppliedAck`, `DDLRejected`.

**Acceptance**:
- ADD COLUMN test: insert 100 rows, ALTER TABLE ADD COLUMN, insert 100 more rows. Verify Iceberg table has the new column; pre-DDL rows have NULL for it; post-DDL rows have correct values.
- RENAME COLUMN test: similar, verify field ID preserved across the rename.
- DROP COLUMN test: verify outcome is Absorbed; Iceberg schema unchanged; new rows write NULL for the dropped column.
- TRUNCATE test: verify changefeed enters error state, sink halts loudly.

### M6 — End-to-end correctness

**Build**:
- A test harness that drives a long-running mixed workload (INSERT/UPDATE/DELETE + ADD COLUMN) against TiDB while running the sink.
- A verification step that compares row counts and content between TiDB (`SET tidb_snapshot = <barrier>; SELECT * FROM <table>`) and the Iceberg CDC log (after applying the I/U/D semantics).

**Acceptance**:
- 1-hour mixed workload at ~1MB/s ingest.
- Zero row count discrepancy between TiDB and Iceberg at the end-of-run barrier.
- Zero column-value CRC discrepancy on a sampled subset.

### Pre-launch gate (after M6)

- All five smoke-test subcommands re-run cleanly (`iceberg-irc-smoke/`).
- Local k3d e2e: 1MB/s sustained for 1h, zero data discrepancies.
- DDL apply: ADD/RENAME/DROP COLUMN, MODIFY COLUMN type-promote (absorbed) all work end-to-end.
- Lease changeover under load: kill the leaseholder during sustained writes; verify correctness invariant 2 (no duplicates) holds.
- Memory/goroutine baseline: 200 changefeeds, 6h soak; no leaks.

## 18. Open questions

- **TableInfoVersion catch-up on dispatcher restart**: when a dispatcher restarts and replays from an earlier startTs, may receive `TableInfo` versions newer than what the committer has applied. Need a "diff-and-catch-up" step in the committer. May surface during M5; defer until empirically observed.
- **REST catalog implementations vary**: some support Iceberg's vended-credentials, some don't; some support snapshot properties, some have bugs. Validate each catalog before promising support.

---
