# Development Plan — DataHonDo Flink Streaming Platform

**Reference:** `flink_streaming_framework_architecture.svg`
**Date:** 2026-06-05
**Status:** Planning

---

## Architecture Gap Analysis

The architecture diagram defines five capability pillars. The table below maps each pillar to its current implementation state.

| Pillar | Diagram shows | Implemented | Gap |
|--------|--------------|-------------|-----|
| **Soft sources** | Kafka/EventHub, API/webhook | Kafka ✅ | API/webhook source ❌ |
| **Hard sources** | JDBC/DB, File/batch | — | Both missing ❌ |
| **Schema validation** | JSON · Avro, Schema Registry | JSON + Avro inline ✅ | Schema Registry ❌ |
| **Common data model** | Flink Table API, unified schema | TransformationLayer ✅ | — |
| **SQL/HQL transform** | .hql/.sql files, Table API exec | TransformationLayer ✅ | — |
| **Hot sink** | Kafka · API sink | Kafka ✅ | API/REST sink ❌ |
| **Warm sink** | JDBC · DB sink | Audit/recon only | Pipeline JDBC sink ❌ |
| **Cold sink** | Iceberg · File sink | — | Both missing ❌ |
| **Audit** | Job start/end, read/write counts | AuditService ✅ | — |
| **Reconciliation** | Src/target count, mismatch | ReconciliationService ✅ | — |
| **Checkpoint** | Docker volume, restart strategy | SavepointService ✅ | — |
| **Metrics** | Throughput, Latency, Error count | Accumulators only 🔶 | Metrics reporter ❌ |

---

## Roadmap

### Feature 009 — Additional Source Layers
**Priority: HIGH** | Unblocks hard data layer and JDBC/file pipelines

### Feature 010 — Additional Sink Layers (Warm + Cold zones)
**Priority: HIGH** | Completes the data storage sandbox

### Feature 011 — Schema Registry Integration
**Priority: MEDIUM** | Required for enterprise Avro pipelines

### Feature 012 — Metrics & Observability
**Priority: MEDIUM** | Throughput/latency/error visibility beyond accumulators

---

## Feature 009 — Additional Source Layers

### 009-A: File / Batch Source (`FileSourceLayer`)

**What:** Read CSV / JSON / Parquet files from a local or ADLS/S3 path.
Registers a Flink `FileSource` (1.14+ API) as a Flink Table view —
identical interface to `KafkaSourceLayer` so the orchestrator needs no changes.

**Classes to create:**

| Class | Package | Role |
|-------|---------|------|
| `FileSourceLayer` | `source` | Implements `SourceLayer`; reads via `FileSystem.get()` |
| `FileSourceConfig` fields | `config.SourceConfig` | `filePath`, `fileFormat` (CSV/JSON/PARQUET), `recursive` |
| `FileSourceLayerTest` | `source` (test) | Schema validation, field mapping, missing-file error |

**Config YAML shape:**
```yaml
streaming.job.sources[0]:
  type: FILE
  tableName: orders
  filePath: /app/data/orders.csv
  fileFormat: CSV       # CSV | JSON | PARQUET
  schema:
    fields:
      - { name: id,    type: INT }
      - { name: name,  type: STRING }
```

**Key implementation points:**
- `SourceConfig.type` discriminator: `KAFKA` (existing) | `FILE` | `JDBC` | `API`
- Register as `StreamTableEnvironment.createTemporaryView(tableName, ...)`
- Schema-driven column projection via `RowTypeInfo` (same as `KafkaSourceLayer`)
- DLQ side-output applies: malformed rows → `DLQ_TAG`

**Acceptance tests:**
- `createSourceTable_registersView_forCsvFile`
- `createSourceTable_throwsSchemaException_whenRequiredFieldMissing`
- `createSourceTable_throwsIllegalArgument_whenFilePathIsNull`

---

### 009-B: JDBC / DB Source (`JdbcSourceLayer`)

**What:** Read from any JDBC datasource (PostgreSQL, MySQL, Oracle).
Uses Flink's `JdbcInputFormat` wrapped in a `fromDataStream()` → Table view.

**Classes to create:**

| Class | Package | Role |
|-------|---------|------|
| `JdbcSourceLayer` | `source` | Implements `SourceLayer`; `JdbcInputFormat` builder |
| `JdbcSourceConfig` fields | `config.SourceConfig` | `jdbcUrl`, `query`, `fetchSize`, `authConfig` |
| `JdbcSourceLayerTest` | `source` (test) | Mock JDBC connection, field mapping |

**Config YAML shape:**
```yaml
streaming.job.sources[0]:
  type: JDBC
  tableName: customers
  jdbcUrl: jdbc:postgresql://host:5432/db
  query: "SELECT id, name, email FROM customers WHERE active = true"
  fetchSize: 1000
  auth:
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
```

**Key implementation points:**
- Uses `flink-connector-jdbc` (already in pom.xml transitive — confirm)
- `fetchSize` controls parallelism-aware partitioned reads
- Schema inferred from `ResultSetMetaData` then cross-validated against `SourceConfig.schema`

**Acceptance tests:**
- `createSourceTable_registersView_forJdbcQuery`
- `createSourceTable_throwsIllegalArgument_whenJdbcUrlIsNull`
- `createSourceTable_throwsIllegalArgument_whenQueryIsNull`

---

### 009-C: API / Webhook Source (`ApiSourceLayer`)

**What:** Poll a REST endpoint on a configurable interval and feed responses
into the Flink stream. Implemented as a `SourceFunction` (or `Source` with
`CheckpointedFunction` for at-least-once).

**Classes to create:**

| Class | Package | Role |
|-------|---------|------|
| `ApiSourceLayer` | `source` | Implements `SourceLayer` |
| `RestPollingSourceFunction` | `source` | `RichSourceFunction<String>` with polling loop |
| `ApiSourceConfig` fields | `config.SourceConfig` | `url`, `method`, `headers`, `pollIntervalMs`, `jsonPath` |
| `ApiSourceLayerTest` | `source` (test) | Mock HTTP, rate limit, JSON extraction |

**Config YAML shape:**
```yaml
streaming.job.sources[0]:
  type: API
  tableName: prices
  url: https://api.example.com/prices
  method: GET
  pollIntervalMs: 5000
  jsonPath: $.data[*]     # JSONPath to extract records array
  headers:
    Authorization: Bearer ${API_TOKEN}
```

**Key implementation points:**
- Uses `java.net.http.HttpClient` (Java 11+) — note: project targets Java 8,
  so use `java.net.HttpURLConnection` or Apache HttpClient
- `jsonPath` extraction with JsonPath library (add to pom.xml)
- At-least-once: checkpoint saves last-polled cursor/timestamp
- DLQ for HTTP errors (4xx/5xx) → `DLQ_TAG`

**Acceptance tests:**
- `createSourceTable_registersView_afterSuccessfulPoll`
- `createSourceTable_routesToDlq_on4xxResponse`
- `createSourceTable_retries_onTransientError`

---

### 009 — Orchestrator changes

`StreamingJobOrchestrator.buildSourceLayer()` currently hardcodes `KafkaSourceLayer`.
Replace with a dispatcher:

```java
SourceLayer resolveSourceLayer(SourceConfig config) {
    switch (config.getType().toUpperCase()) {
        case "FILE":  return fileSourceLayer;
        case "JDBC":  return jdbcSourceLayer;
        case "API":   return apiSourceLayer;
        default:      return kafkaSourceLayer;   // existing
    }
}
```

All `SourceLayer` beans injected via Spring `@Autowired` list or explicit fields.

---

## Feature 010 — Additional Sink Layers

### 010-A: JDBC Sink (`JdbcTargetLayer`) — Warm zone

**What:** Write the result `Table` to any JDBC datasource.
Uses Flink's `JdbcSink.sink()` connector.

**Classes to create:**

| Class | Package | Role |
|-------|---------|------|
| `JdbcTargetLayer` | `sink` | Implements `TargetLayer`; `getSinkType()` → `"JDBC"` |
| `JdbcTargetLayerTest` | `sink` (test) | Mock JDBC, upsert mode, batch size |

**Config YAML shape:**
```yaml
streaming.job.target:
  type: JDBC
  tableName: output_orders
  jdbcUrl: jdbc:postgresql://host:5432/db
  batchSize: 500
  batchIntervalMs: 1000
  upsertMode: false       # true = INSERT … ON CONFLICT DO UPDATE
  auth:
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
```

**Key implementation points:**
- `JdbcSink.sink(...)` from `flink-connector-jdbc`
- Schema-aware `JdbcStatementBuilder<Row>` — derives INSERT from `TargetConfig.schema.fields`
- Upsert mode: `INSERT … ON CONFLICT (pk) DO UPDATE SET …` (PostgreSQL dialect)
- Audit: `TARGET_WRITTEN` accumulator incremented in `JdbcRowStatementBuilder`

**Acceptance tests:**
- `sink_buildsInsertStatement_fromSchemaFields`
- `sink_throwsIllegalArgument_whenJdbcUrlIsNull`
- `sink_throwsIllegalArgument_whenTableNameIsNull`
- `sink_buildUpsertStatement_whenUpsertModeEnabled`

---

### 010-B: File / CSV Sink (`FileTargetLayer`) — Cold zone

**What:** Write the result `Table` to the filesystem (local or ADLS/S3/GCS).
Uses Flink's `FileSink` API with rolling policy.

**Classes to create:**

| Class | Package | Role |
|-------|---------|------|
| `FileTargetLayer` | `sink` | Implements `TargetLayer`; `getSinkType()` → `"FILE"` |
| `FileTargetLayerTest` | `sink` (test) | Rolling policy, format selection |

**Config YAML shape:**
```yaml
streaming.job.target:
  type: FILE
  outputPath: /app/output/orders
  fileFormat: CSV         # CSV | JSON | PARQUET
  rollOnCheckpoint: true
  maxFileSizeBytes: 134217728   # 128 MB
```

**Key implementation points:**
- `FileSink.forRowFormat(path, encoder)` for CSV/JSON
- `FileSink.forBulkFormat(path, writer)` for Parquet (requires `flink-parquet`)
- Rolling policy: by checkpoint (`OnCheckpointRollingPolicy`) or by file size
- Output path templating: `{outputPath}/{jobName}/{date}/part-{taskId}`

**Acceptance tests:**
- `sink_createsFile_withExpectedRows_forCsvFormat`
- `sink_throwsIllegalArgument_whenOutputPathIsNull`
- `sink_rollsFile_onCheckpoint`

---

### 010-C: API / REST Sink (`ApiTargetLayer`) — Hot zone extension

**What:** POST each output row as JSON to a REST endpoint.
Implemented as an `AsyncDataStream` sink using `AsyncFunction` for throughput.

**Classes to create:**

| Class | Package | Role |
|-------|---------|------|
| `ApiTargetLayer` | `sink` | Implements `TargetLayer`; `getSinkType()` → `"API"` |
| `HttpRowSinkFunction` | `sink` | `RichSinkFunction<Row>` with retry |
| `ApiTargetLayerTest` | `sink` (test) | Mock HTTP, retry on 5xx, DLQ on 4xx |

**Config YAML shape:**
```yaml
streaming.job.target:
  type: API
  url: https://api.example.com/ingest
  method: POST
  headers:
    Authorization: Bearer ${API_TOKEN}
  retryAttempts: 3
  retryBackoffMs: 500
  dlq:
    enabled: true
    topic: api-sink-dlq
    bootstrapServers: kafka:29092
```

**Acceptance tests:**
- `sink_postsRow_asJson_toEndpoint`
- `sink_retries_on5xxResponse`
- `sink_routesToDlq_on4xxAfterRetries`

---

### 010 — Orchestrator / Controller changes

`TargetLayer` already has `getSinkType()`. `StreamingJobOrchestrator` resolves
by iterating injected `List<TargetLayer>` beans and matching `getSinkType()`:

```java
TargetLayer resolveTargetLayer(TargetConfig config) {
    return targetLayers.stream()
        .filter(t -> t.getSinkType().equalsIgnoreCase(config.getType()))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            "No TargetLayer for type: " + config.getType()));
}
```

`JobController.validateJob()` must validate target config per sink type
(JDBC URL, file path, API URL) before submission.

---

## Feature 011 — Schema Registry Integration

### Overview

Replace inline JSON/Avro schema with Confluent Schema Registry lookup.
Schema is fetched by subject (`<topic>-value`) and cached.

### Classes to create

| Class | Package | Role |
|-------|---------|------|
| `SchemaRegistryClient` | `source` | Fetches Avro schema from Registry REST API |
| `CachedSchemaRegistryClient` | `source` | TTL-based in-memory cache wrapping `SchemaRegistryClient` |
| `SchemaRegistryConfig` fields | `config.SourceConfig` | `registryUrl`, `subject`, `cacheTtlMs` |
| `SchemaRegistryClientTest` | `source` (test) | Mock registry, 404 handling, cache eviction |

### Config YAML shape

```yaml
streaming.job.sources[0]:
  type: KAFKA
  topic: orders
  schemaDefinition:
    type: REGISTRY             # INLINE (existing) | REGISTRY (new)
    registryUrl: http://schema-registry:8081
    subject: orders-value      # defaults to <topic>-value if omitted
    cacheTtlMs: 300000         # 5 minutes
```

### Key implementation points

- `KafkaSourceLayer` checks `SchemaConfig.type`: `REGISTRY` → `CachedSchemaRegistryClient`,
  `INLINE` → existing inline path (no regression)
- Schema fetched once per source init, then cached
- Cache invalidation: TTL expiry OR explicit `/api/schema/refresh/{jobName}` endpoint
- Failed registry fetch → `DLQ_TAG` for that batch, not a hard failure
- `SqlValidatorService` must also call registry when `type: REGISTRY` to validate SQL

### Acceptance tests

- `fetchSchema_returnsAvroSchema_forKnownSubject`
- `fetchSchema_throwsSchemaException_forUnknownSubject`
- `cachedClient_returnsFromCache_onSecondCall`
- `cachedClient_refreshes_afterTtlExpiry`

---

## Feature 012 — Metrics & Observability

### Overview

Expose pipeline throughput, latency, error rate, and sink success/failure
via Flink's built-in Metrics system, wired to Prometheus for Grafana dashboards.

### Components

| Component | Role |
|-----------|------|
| `FlinkMetricsConfig` | `prometheusPort`, `metricsEnabled` config fields |
| `PrometheusReporter` config | Added to `flink-conf.yaml` / `FLINK_PROPERTIES` |
| `MetricsService` | Spring bean wrapping Flink `MetricGroup` for app-level metrics |
| `PipelineMetrics` | Constants for metric names (throughput, latency, errors) |
| Grafana dashboard JSON | Pre-built dashboard for the 4 cross-cutting metrics |

### Metrics to expose

| Metric | Type | Description |
|--------|------|-------------|
| `pipeline.source.records_read` | Counter | Records consumed per source per job |
| `pipeline.source.records_rejected` | Counter | Schema-rejected records |
| `pipeline.transform.records_out` | Counter | Records emitted after SQL transform |
| `pipeline.sink.records_written` | Counter | Records accepted by sink |
| `pipeline.sink.records_failed` | Counter | Records the sink rejected / errored |
| `pipeline.latency.p50_ms` | Gauge | Median end-to-end event latency |
| `pipeline.latency.p99_ms` | Gauge | 99th percentile latency |
| `pipeline.dlq.records_routed` | Counter | Records sent to DLQ |
| `pipeline.checkpoint.duration_ms` | Gauge | Last checkpoint duration |

### Config YAML shape

```yaml
streaming:
  job:
    flink:
      metrics-enabled: ${METRICS_ENABLED:true}
      prometheus-port: ${PROMETHEUS_PORT:9249}
```

**`docker-compose.yml` additions:**
```yaml
prometheus:
  image: prom/prometheus:latest
  volumes:
    - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
  ports:
    - "9090:9090"

grafana:
  image: grafana/grafana:latest
  ports:
    - "3000:3000"
  volumes:
    - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards
```

### Acceptance tests

- `metricsService_incrementsSourceRead_onEachRecord`
- `metricsService_incrementsSinkFailed_onSinkException`
- `prometheusEndpoint_exposesAllPipelineMetrics`

---

## Implementation Order & Dependencies

```
Feature 009-A (File source)   ─┐
Feature 009-B (JDBC source)   ─┤── no external deps beyond flink-connector-jdbc
Feature 009-C (API source)    ─┘

Feature 010-A (JDBC sink)     ─── depends on flink-connector-jdbc (shared with 009-B)
Feature 010-B (File sink)     ─── depends on flink-parquet (new dep for PARQUET format)
Feature 010-C (API sink)      ─── no new deps

Feature 011 (Schema Registry) ─── depends on confluent schema-registry-client (new dep)

Feature 012 (Metrics)         ─── depends on flink-metrics-prometheus (new dep)
                                   best done after 009+010 to capture all metric points
```

**Recommended sequencing:**

| Sprint | Features | Deliverable |
|--------|----------|-------------|
| 1 | 009-A (File source) + 010-A (JDBC sink) | Hard data layer: file→JDBC pipeline |
| 2 | 009-B (JDBC source) + 010-B (File sink) | JDBC→file and JDBC→JDBC pipelines |
| 3 | 009-C (API source) + 010-C (API sink) | API polling + REST push |
| 4 | 011 (Schema Registry) | Enterprise Avro registry integration |
| 5 | 012 (Metrics) | Full Prometheus/Grafana observability |

---

## New `pom.xml` dependencies required

| Artifact | Version | Feature |
|----------|---------|---------|
| `flink-connector-jdbc` | `3.1.2-1.17` | 009-B, 010-A |
| `postgresql` driver | `42.7.x` | 009-B, 010-A |
| `flink-parquet` | `1.18.0` | 010-B |
| `hadoop-common` | `3.3.x` (provided) | 010-B (Parquet writer) |
| `confluent schema-registry-client` | `7.5.x` | 011 |
| `flink-metrics-prometheus` | `1.18.0` | 012 |
| `jsonpath` (Jayway) | `2.9.x` | 009-C |

---

## TDD Coverage Targets (per WORK_AGREEMENT)

Each new feature must reach:
- **≥ 80%** unit-tested features (✅ in FEATURE_COVERAGE_MATRIX)
- **≥ 1** integration test per new source/sink type
- **0** features with 🚧 Stub status at PR merge

New feature doc created in `src/docs/features/00X-<name>.md` before coding begins.
FEATURE_COVERAGE_MATRIX.md updated at feature completion.

---

## Open Questions (to resolve before Feature 009 coding)

1. **File path base:** Local filesystem or ADLS Gen2 / S3?
   — Determines if `azure-storage-datalake` dependency is needed.
2. **JDBC sink upsert key:** Who provides the PK column list — config or schema?
3. **API source auth:** Only Bearer token, or also mutual TLS / API key header?
4. **Schema Registry auth:** Is the internal registry open or SASL-secured?
5. **Metrics retention:** Prometheus scrape interval and data retention window?

---

*Maintained by: DataHonDo Engineering*
*Last updated: 2026-06-05*
