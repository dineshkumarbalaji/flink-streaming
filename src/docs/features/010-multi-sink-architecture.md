# Feature 010 — Multi-Sink Architecture

**Feature ID:** 010
**Status:** Done
**Type:** Feature
**Date:** 2026-06-05

---

## Overview

Extends the sink layer from a single Kafka implementation to a pluggable multi-sink
architecture covering all three storage zones defined in the architecture diagram:
**Hot** (Kafka + API), **Warm** (JDBC), and **Cold** (File/Parquet).

---

## Sink Type Discriminator

`TargetConfig.type` selects the implementation at job-submit time:

| Value | Layer class | Storage zone |
|-------|-------------|-------------|
| `KAFKA` (default) | `KafkaTargetLayer` | Hot — event streaming |
| `JDBC` | `JdbcTargetLayer` | Warm — relational database |
| `FILE` | `FileTargetLayer` | Cold — filesystem / data lake |
| `API` | `ApiTargetLayer` | Hot — REST push |

---

## 010-A: JDBC Sink (`JdbcTargetLayer`) — Warm zone

### What it does
Writes the result Table to any JDBC datasource using INSERT or upsert semantics.
Uses Flink's Table API JDBC connector DDL. Upsert key is resolved in priority order:

1. `target.upsertKeyColumns` (explicit config list)
2. `schema.fields[].primaryKey: true` (schema-derived)
3. No key → plain `INSERT` (upsert mode disabled or no key resolvable)

### Dialect-specific upsert SQL

| Dialect | Upsert template |
|---------|----------------|
| `POSTGRESQL` (auto-detected) | `INSERT … ON CONFLICT (pk) DO UPDATE SET col=EXCLUDED.col` |
| `MYSQL` | `INSERT … ON DUPLICATE KEY UPDATE col=VALUES(col)` |
| `ORACLE` | `MERGE INTO t USING dual ON (pk=?) WHEN MATCHED THEN UPDATE … WHEN NOT MATCHED THEN INSERT …` |
| `H2` | `MERGE INTO t (…) KEY (pk) VALUES (…)` |

Dialect is auto-detected from the JDBC URL prefix. Override with `target.jdbcDialect`.

### Configuration

```yaml
streaming.job.target:
  type: JDBC
  tableName: output_orders
  jdbcUrl: jdbc:postgresql://host:5432/mydb
  batchSize: 500
  batchIntervalMs: 1000
  upsertMode: true
  upsertKeyColumns: [id, tenant_id]   # overrides schema primaryKey fields
  jdbcUsername: ${DB_USERNAME}
  jdbcPassword: ${DB_PASSWORD}
  sslMode: require
  schema:
    fields:
      - { name: id,        type: INT,    primaryKey: true }
      - { name: tenant_id, type: STRING, primaryKey: true }
      - { name: amount,    type: DOUBLE }
```

### Key classes

| Class | Role |
|-------|------|
| `JdbcTargetLayer` | Implements `TargetLayer`; `getSinkType()` → `"JDBC"`; resolves dialect and key columns |
| `JdbcDialect` | Enum: POSTGRESQL/MYSQL/ORACLE/H2; generates dialect-specific INSERT and upsert SQL |

---

## 010-B: File Sink (`FileTargetLayer`) — Cold zone

### What it does
Writes the result Table to local filesystem, Azure ADLS Gen2, or AWS S3 in CSV, JSON,
or Parquet format. Uses Flink's Table API `filesystem` connector with checkpoint-based
or size-based rolling policy.

### Storage backends

Same URI scheme detection as Feature 009-A (`StoragePathResolver`):

| URI prefix | Backend |
|------------|---------|
| Bare path or `file:///` | Local |
| `abfs://` / `abfss://` | Azure ADLS Gen2 |
| `s3://` / `s3a://` | AWS S3 |

### Configuration

```yaml
streaming.job.target:
  type: FILE
  fileFormat: PARQUET           # CSV | JSON | PARQUET
  storagePath: s3a://my-bucket/output/orders
  rollOnCheckpoint: true        # roll output file on each Flink checkpoint
  maxFileSizeBytes: 134217728   # 128 MB; 0 = checkpoint-only rolling
  partitionBy: date             # optional: output/{date}/part-{taskId}
  storage:
    s3:
      accessKey: ${AWS_ACCESS_KEY}
      secretKey: ${AWS_SECRET_KEY}
      region: eu-west-1
```

### Key classes

| Class | Role |
|-------|------|
| `FileTargetLayer` | Implements `TargetLayer`; `getSinkType()` → `"FILE"`; builds filesystem DDL |
| `StoragePathResolver` | Shared with 009-A; normalises path, injects FS credentials |

---

## 010-C: API Sink (`ApiTargetLayer`) — Hot zone extension

### What it does
POSTs each output row as JSON to a REST endpoint. Supports individual records or batched
JSON arrays. Retries on 5xx with exponential backoff. Non-retryable 4xx errors can be
routed to a DLQ Kafka topic.

### Authentication
Shares `ApiAuthConfig` with Feature 009-C: BEARER, OAUTH2, MTLS, API_KEY — same fields,
same `HttpClientFactory` / `OAuthTokenManager` infrastructure.

### Configuration

```yaml
streaming.job.target:
  type: API
  url: https://api.example.com/ingest
  method: POST
  apiBatchSize: 1               # 1 = per-record; >1 = JSON array batch
  retryAttempts: 3
  retryBackoffMs: 500           # exponential: 500, 1000, 2000 ms
  connectTimeoutMs: 5000
  readTimeoutMs: 10000
  apiAuth:
    type: BEARER
    token: ${API_BEARER_TOKEN}
  dlq:
    enabled: true
    topic: api-sink-dlq
    bootstrapServers: kafka:29092
```

### Key classes

| Class | Role |
|-------|------|
| `ApiTargetLayer` | Implements `TargetLayer`; `getSinkType()` → `"API"`; validates config; wires sink |
| `HttpRowSinkFunction` | `RichSinkFunction<Row>`; serialises Row to JSON; POSTs with retry; buffers for batch |

---

## Orchestrator Dispatch

Mirrors the source-layer pattern (see Feature 009). Spring injects all `TargetLayer` beans
as a `List<TargetLayer>` keyed by `getSinkType()`:

```java
@Autowired
public StreamingJobOrchestrator(List<TargetLayer> targetLayerList, ...) {
    this.targetLayers = targetLayerList.stream()
        .collect(Collectors.toMap(t -> t.getSinkType().toUpperCase(), t -> t));
}
```

`getSinkType()` was already declared on the `TargetLayer` interface — no interface change
needed. `KafkaTargetLayer` returns `"KAFKA"` (unchanged); new layers return their type.

---

## Validation (JobController)

| Type | Validated fields |
|------|-----------------|
| `JDBC` | `jdbcUrl` non-blank; `tableName` non-blank; if `upsertMode=true`, key must resolve |
| `FILE` | `storagePath` non-blank; `fileFormat` in {CSV, JSON, PARQUET} |
| `API` | `url` valid URI; `apiAuth.type` set; OAUTH2 requires `tokenUrl` |

---

## Test Coverage

| Test class | Methods | Coverage |
|------------|---------|---------|
| `JdbcDialectTest` | 8 | Auto-detect; PostgreSQL/MySQL INSERT and upsert SQL; placeholder count |
| `JdbcTargetLayerTest` | 5 | Validation: null URL, null table, upsert with no key; config key priority |
| `FileTargetLayerTest` | 4 | Validation: null/blank path, invalid format |
| `ApiTargetLayerTest` | 5 | Validation: null/blank URL, OAUTH2 missing tokenUrl, invalid URI |

---

## Known Limitations

- `FileTargetLayer` Parquet support requires the Flink Parquet plugin on the cluster.
- `ApiTargetLayer` DLQ routing logs 4xx responses but full Kafka side-output routing
  is deferred to a future iteration (model is in place in `TargetConfig.dlq`).
- `JdbcTargetLayer` Oracle upsert uses MERGE INTO which requires the Oracle JDBC driver
  on the cluster — not bundled in pom.xml to avoid license issues.
