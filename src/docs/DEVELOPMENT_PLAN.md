# Development Plan — DataHonDo Flink Streaming Platform

**Reference:** `flink_streaming_framework_architecture.svg`
**Date:** 2026-06-05
**Status:** Approved for Development

---

## Resolved Design Decisions

| Question | Decision |
|----------|----------|
| File storage target | **All** — Local filesystem, Azure ADLS Gen2, and AWS S3 |
| JDBC sink upsert key | **Both** — config-provided `upsertKeyColumns` with fallback to `schema.fields[].primaryKey: true` |
| API source/sink auth | **All** — Bearer token, OAuth2 client credentials, mTLS, and API key header |
| Schema Registry auth | **SASL-secured** — PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 + optional TLS |
| Prometheus retention | **Yes** — 30-day retention, 15s scrape interval, Grafana dashboards provisioned |

---

## Architecture Gap Analysis

| Pillar | Diagram shows | Implemented | Gap |
|--------|--------------|-------------|-----|
| **Soft sources** | Kafka/EventHub, API/webhook | Kafka ✅ | API/webhook source ❌ |
| **Hard sources** | JDBC/DB, File/batch | — | Both missing ❌ |
| **Schema validation** | JSON · Avro, Schema Registry | JSON + Avro inline ✅ | Schema Registry ❌ |
| **Common data model** | Flink Table API, unified schema | TransformationLayer ✅ | — |
| **SQL/HQL transform** | .hql/.sql files, Table API exec | TransformationLayer ✅ | — |
| **Hot sink** | Kafka · API sink | Kafka ✅ | API/REST sink ❌ |
| **Warm sink** | JDBC · DB sink | Audit/recon JDBC only | Pipeline JDBC sink ❌ |
| **Cold sink** | Iceberg · File sink | — | Both missing ❌ |
| **Audit** | Job start/end, read/write counts | AuditService ✅ | — |
| **Reconciliation** | Src/target count, mismatch | ReconciliationService ✅ | — |
| **Checkpoint** | Docker volume, restart strategy | SavepointService ✅ | — |
| **Metrics** | Throughput, Latency, Error count | Accumulators only 🔶 | Metrics reporter ❌ |

---

## Roadmap

| Feature | Scope | Priority | Sprint |
|---------|-------|----------|--------|
| **009-A** | File source — Local / ADLS Gen2 / S3 | HIGH | 1 |
| **010-A** | JDBC sink — Warm zone (upsert support) | HIGH | 1 |
| **009-B** | JDBC source — PostgreSQL / MySQL / Oracle | HIGH | 2 |
| **010-B** | File sink — CSV / JSON / Parquet + storage tiers | HIGH | 2 |
| **009-C** | API source — Bearer / OAuth2 / mTLS / API-key | MEDIUM | 3 |
| **010-C** | API sink — REST push with retry + DLQ | MEDIUM | 3 |
| **011** | Schema Registry — SASL-secured Confluent | MEDIUM | 4 |
| **012** | Metrics — Prometheus / Grafana / 30-day retention | MEDIUM | 5 |

---

## Feature 009-A — File / Batch Source (`FileSourceLayer`)

### What

Read CSV / JSON / Parquet files from **local filesystem, Azure ADLS Gen2, or AWS S3**.
Registers a Flink `FileSource` view — same `SourceLayer` interface as `KafkaSourceLayer`.

### Storage URI scheme detection

| Prefix | Backend | Auth |
|--------|---------|------|
| `file:///` or bare path | Local filesystem | None |
| `abfs://<container>@<account>.dfs.core.windows.net/` | ADLS Gen2 | Storage account key OR service principal |
| `s3://<bucket>/` or `s3a://<bucket>/` | AWS S3 | Access key + secret OR IAM role |

### Classes to create

| Class | Package | Role |
|-------|---------|------|
| `FileSourceLayer` | `source` | Implements `SourceLayer`; delegates to Flink `FileSource` |
| `StoragePathResolver` | `source` | Detects URI scheme, configures `FileSystem` credentials |
| `FileSourceLayerTest` | `source` (test) | Local file happy path, missing file, schema mismatch |
| `StoragePathResolverTest` | `source` (test) | URI scheme detection for all 3 backends |

### `SourceConfig` additions

```yaml
streaming.job.sources[0]:
  type: FILE                          # KAFKA | FILE | JDBC | API
  tableName: orders
  fileFormat: CSV                     # CSV | JSON | PARQUET
  storagePath: abfs://raw@myaccount.dfs.core.windows.net/orders/2026/
  recursive: true                     # scan subdirectories
  monitorInterval: 0                  # 0 = one-shot batch; >0 = continuous watching (ms)
  storage:
    adls:
      accountName: ${ADLS_ACCOUNT}
      accountKey: ${ADLS_KEY}         # OR use servicePrincipal below
      servicePrincipal:
        tenantId: ${AZURE_TENANT_ID}
        clientId: ${AZURE_CLIENT_ID}
        clientSecret: ${AZURE_CLIENT_SECRET}
    s3:
      accessKey: ${AWS_ACCESS_KEY}
      secretKey: ${AWS_SECRET_KEY}
      region: ${AWS_REGION:eu-west-1}
      endpoint: ${S3_ENDPOINT:}       # empty = AWS; set for MinIO / compatible
  schema:
    fields:
      - { name: id,    type: INT }
      - { name: name,  type: STRING }
```

### Key implementation points

- `StoragePathResolver.configure(env, storagePath, storageConfig)` calls
  `env.getConfiguration().set(...)` to inject Hadoop-compatible FS credentials
  before `FileSource` is built (ADLS: `fs.azure.account.key.*`, S3: `fs.s3a.access.key`)
- CSV via `CsvReaderFormat.forPojo(...)` or `TextLineInputFormat` + schema mapping
- JSON via `JsonRowDeserializationSchema`
- Parquet via `ParquetColumnarRowInputFormat`
- `monitorInterval > 0` → `FileSource.forRecordStreamFormat(...).monitorContinuously(...)`
- DLQ side-output on malformed rows (same `DLQ_TAG` as Kafka path)

### Acceptance tests

- `createSourceTable_registersView_forLocalCsvFile`
- `createSourceTable_registersView_forAdlsPath_withAccountKey`
- `createSourceTable_registersView_forS3Path`
- `createSourceTable_throwsIllegalArgument_whenStoragePathIsNull`
- `createSourceTable_throwsSchemaException_whenRequiredFieldMissing`
- `storagePathResolver_detectsAdlsScheme`
- `storagePathResolver_detectsS3Scheme`
- `storagePathResolver_detectsLocalScheme`

---

## Feature 009-B — JDBC Source (`JdbcSourceLayer`)

### What

Read from any JDBC datasource (PostgreSQL, MySQL, Oracle) into a Flink Table view.
Uses `JdbcInputFormat` wrapped in `fromDataSource()` → `createTemporaryView()`.

### Classes to create

| Class | Package | Role |
|-------|---------|------|
| `JdbcSourceLayer` | `source` | Implements `SourceLayer`; `JdbcInputFormat` builder |
| `JdbcRowTypeMapper` | `source` | Maps `ResultSetMetaData` → `RowTypeInfo`, cross-validates with schema config |
| `JdbcSourceLayerTest` | `source` (test) | Mock JDBC, field mapping, partition query |

### `SourceConfig` additions

```yaml
streaming.job.sources[0]:
  type: JDBC
  tableName: customers
  jdbcUrl: jdbc:postgresql://host:5432/mydb
  query: "SELECT id, name, email FROM customers WHERE active = true"
  fetchSize: 1000
  numPartitions: 4                    # parallel read partitions
  partitionColumn: id                 # used with lowerBound/upperBound
  lowerBound: 1
  upperBound: 1000000
  auth:
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
    sslMode: require                  # disable | require | verify-full
    sslCertPath: ${DB_SSL_CERT_PATH:}
```

### Key implementation points

- Driver class auto-detected from URL prefix (`jdbc:postgresql:` → `org.postgresql.Driver`, etc.)
- Partitioned reads: when `numPartitions > 1`, generates `WHERE id >= X AND id < Y` sub-queries
- Schema cross-validation: `ResultSetMetaData` column names/types validated against `schema.fields`
- SSL: `sslMode=require` sets `ssl=true` on the connection properties

### Acceptance tests

- `createSourceTable_registersView_forJdbcQuery`
- `createSourceTable_partitionsQuery_whenNumPartitionsConfigured`
- `createSourceTable_throwsIllegalArgument_whenJdbcUrlIsNull`
- `createSourceTable_throwsIllegalArgument_whenQueryIsNull`
- `jdbcRowTypeMapper_mapsPostgresTypes_correctly`
- `jdbcRowTypeMapper_throwsSchemaException_whenColumnMissing`

---

## Feature 009-C — API / Webhook Source (`ApiSourceLayer`)

### What

Poll a REST endpoint on a configurable interval. Supports four auth mechanisms.
Implemented as `RichSourceFunction<String>` with checkpoint-backed cursor for at-least-once.

### Auth model

| Type | Config fields | Mechanism |
|------|--------------|-----------|
| `BEARER` | `token` | Static `Authorization: Bearer <token>` header |
| `OAUTH2` | `tokenUrl`, `clientId`, `clientSecret`, `scope` | Client credentials flow; token refreshed before expiry |
| `MTLS` | `keystorePath`, `keystorePassword`, `truststorePath`, `truststorePassword` | Client certificate presented on TLS handshake |
| `API_KEY` | `apiKey`, `apiKeyHeader` (default: `X-Api-Key`), `apiKeyLocation` (`HEADER`\|`QUERY`) | API key in header or query parameter |

### Classes to create

| Class | Package | Role |
|-------|---------|------|
| `ApiSourceLayer` | `source` | Implements `SourceLayer`; wires `RestPollingSourceFunction` |
| `RestPollingSourceFunction` | `source` | `RichSourceFunction<String>` with polling loop + checkpoint state |
| `ApiAuthConfig` | `config` | Auth type enum + all auth fields; used by source AND sink |
| `OAuthTokenManager` | `source` | Client credentials flow; caches token, refreshes 60 s before expiry |
| `HttpClientFactory` | `source` | Builds `HttpClient` (Apache HttpClient 4.x for Java 8); wires mTLS keystore |
| `ApiSourceLayerTest` | `source` (test) | Mock HTTP server, each auth type, DLQ on 4xx |
| `OAuthTokenManagerTest` | `source` (test) | Token fetch, refresh on expiry, retry on failure |

### `SourceConfig` additions

```yaml
streaming.job.sources[0]:
  type: API
  tableName: prices
  url: https://api.example.com/prices
  method: GET
  pollIntervalMs: 5000
  jsonPath: $.data[*]               # JSONPath to extract records array; omit for root array
  connectTimeoutMs: 5000
  readTimeoutMs: 10000
  retryAttempts: 3
  retryBackoffMs: 500
  apiAuth:
    type: OAUTH2                    # BEARER | OAUTH2 | MTLS | API_KEY
    # BEARER
    token: ${API_BEARER_TOKEN:}
    # OAUTH2
    tokenUrl: https://auth.example.com/oauth/token
    clientId: ${OAUTH_CLIENT_ID:}
    clientSecret: ${OAUTH_CLIENT_SECRET:}
    scope: read:prices
    # MTLS
    keystorePath: /app/certs/client.p12
    keystorePassword: ${MTLS_KEYSTORE_PASSWORD:}
    truststorePath: /app/certs/truststore.jks
    truststorePassword: ${MTLS_TRUSTSTORE_PASSWORD:}
    # API_KEY
    apiKey: ${API_KEY:}
    apiKeyHeader: X-Api-Key         # header name
    apiKeyLocation: HEADER          # HEADER | QUERY
```

### Key implementation points

- Uses **Apache HttpClient 4.x** (`httpclient:4.5.x`) — Java 8 compatible
- `OAuthTokenManager` stores access token + expiry in a `volatile` field; refreshes
  proactively 60 s before expiry using a single-threaded scheduler
- mTLS: `SSLContext` loaded from keystore/truststore via `KeyManagerFactory` +
  `TrustManagerFactory`; wired into `HttpClient` via `SSLConnectionSocketFactory`
- Checkpoint state: `ListState<Long>` stores last successfully polled cursor
  (epoch-ms or offset); restored on restart for at-least-once
- DLQ: HTTP 4xx (non-retryable) → `DLQ_TAG`; HTTP 5xx → retry up to `retryAttempts`

### Acceptance tests

- `createSourceTable_registersView_withBearerAuth`
- `createSourceTable_registersView_withOauth2Auth`
- `createSourceTable_registersView_withMtlsAuth`
- `createSourceTable_registersView_withApiKeyHeader`
- `createSourceTable_registersView_withApiKeyQueryParam`
- `createSourceTable_routesToDlq_on4xxResponse`
- `createSourceTable_retries_on5xxResponse_upToMaxAttempts`
- `oauthTokenManager_refreshesToken_beforeExpiry`
- `oauthTokenManager_retries_onTokenFetchFailure`

---

## Feature 009 — Orchestrator changes

`StreamingJobOrchestrator` currently hardcodes `KafkaSourceLayer`.
Replace with a Spring-injected dispatcher:

```java
// Injected via @Autowired List<SourceLayer> — Spring discovers all SourceLayer beans
private final Map<String, SourceLayer> sourceLayers;

@Autowired
public StreamingJobOrchestrator(List<SourceLayer> sourceLayers, ...) {
    this.sourceLayers = sourceLayers.stream()
        .collect(Collectors.toMap(s -> s.getSourceType().toUpperCase(), s -> s));
}

SourceLayer resolveSourceLayer(SourceConfig config) {
    String type = config.getType() == null ? "KAFKA" : config.getType().toUpperCase();
    SourceLayer layer = sourceLayers.get(type);
    if (layer == null) throw new IllegalArgumentException("No SourceLayer for type: " + type);
    return layer;
}
```

`SourceLayer` interface gains default method `getSourceType()` returning `"KAFKA"`
(overridden by `FileSourceLayer` → `"FILE"`, etc.) for backward compatibility.

`JobController.validateJob()` extended to validate per source type:
- `FILE`: `storagePath` not blank, `fileFormat` in allowed set
- `JDBC`: `jdbcUrl` not blank, `query` not blank
- `API`: `url` valid URI, `apiAuth.type` set, required auth fields present

---

## Feature 010-A — JDBC Sink (`JdbcTargetLayer`) — Warm zone

### What

Write the result `Table` to any JDBC datasource with INSERT or upsert semantics.
Uses Flink's `JdbcSink.sink()` connector.

### Upsert key resolution

Priority order:
1. `target.upsertKeyColumns: [id, tenant_id]` — explicit config list
2. `schema.fields` where `primaryKey: true` — schema-derived
3. No upsert key → plain `INSERT` (no conflict handling)

### Classes to create

| Class | Package | Role |
|-------|---------|------|
| `JdbcTargetLayer` | `sink` | Implements `TargetLayer`; `getSinkType()` → `"JDBC"` |
| `JdbcUpsertStatementBuilder` | `sink` | Builds `INSERT … ON CONFLICT (pk) DO UPDATE SET …` per dialect |
| `JdbcInsertStatementBuilder` | `sink` | Builds plain `INSERT INTO … VALUES (…)` |
| `JdbcDialect` | `sink` | Enum: POSTGRESQL, MYSQL, ORACLE — dialect-specific upsert SQL |
| `JdbcTargetLayerTest` | `sink` (test) | Plain INSERT, upsert-by-config, upsert-by-schema, null PK |

### `TargetConfig` additions

```yaml
streaming.job.target:
  type: JDBC
  tableName: output_orders
  jdbcUrl: jdbc:postgresql://host:5432/mydb
  batchSize: 500
  batchIntervalMs: 1000
  upsertMode: true
  upsertKeyColumns: [id, tenant_id]  # optional; falls back to schema primaryKey fields
  auth:
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
    sslMode: require
  schema:
    fields:
      - { name: id,        type: INT,    primaryKey: true }
      - { name: tenant_id, type: STRING, primaryKey: true }
      - { name: amount,    type: DOUBLE }
```

### Dialect-specific upsert SQL

| Dialect | Upsert template |
|---------|----------------|
| PostgreSQL | `INSERT INTO t (…) VALUES (…) ON CONFLICT (pk) DO UPDATE SET col=EXCLUDED.col` |
| MySQL | `INSERT INTO t (…) VALUES (…) ON DUPLICATE KEY UPDATE col=VALUES(col)` |
| Oracle | `MERGE INTO t USING dual ON (pk=?) WHEN MATCHED THEN UPDATE … WHEN NOT MATCHED THEN INSERT …` |

### Acceptance tests

- `sink_buildsInsertStatement_fromSchemaFields`
- `sink_buildsUpsertStatement_fromConfigKeyColumns`
- `sink_buildsUpsertStatement_fromSchemaKeyFields`
- `sink_prefersConfigKeys_overSchemaKeys_whenBothPresent`
- `sink_throwsIllegalArgument_whenJdbcUrlIsNull`
- `sink_throwsIllegalArgument_whenUpsertEnabledButNoKeyResolved`
- `sink_postgresDialect_generatesCorrectUpsertSql`
- `sink_mysqlDialect_generatesCorrectUpsertSql`

---

## Feature 010-B — File Sink (`FileTargetLayer`) — Cold zone

### What

Write the result `Table` to local filesystem, ADLS Gen2, or S3 in CSV, JSON, or Parquet.
Uses Flink's `FileSink` with checkpoint-based rolling policy.

### Classes to create

| Class | Package | Role |
|-------|---------|------|
| `FileTargetLayer` | `sink` | Implements `TargetLayer`; `getSinkType()` → `"FILE"` |
| `FileTargetLayerTest` | `sink` (test) | CSV write, Parquet write, rolling on checkpoint, ADLS path |

### `TargetConfig` additions

```yaml
streaming.job.target:
  type: FILE
  fileFormat: PARQUET              # CSV | JSON | PARQUET
  storagePath: s3a://my-bucket/output/orders
  rollOnCheckpoint: true
  maxFileSizeBytes: 134217728     # 128 MB; 0 = unlimited
  partitionBy: date               # optional: output/{date}/part-{taskId}
  storage:
    s3:
      accessKey: ${AWS_ACCESS_KEY}
      secretKey: ${AWS_SECRET_KEY}
      region: ${AWS_REGION:eu-west-1}
    adls:
      accountName: ${ADLS_ACCOUNT}
      accountKey: ${ADLS_KEY}
```

### Key implementation points

- `StoragePathResolver` (shared with 009-A) configures FS credentials from `storage` config
- CSV/JSON: `FileSink.forRowFormat(path, SimpleStringEncoder)` with `Row.toString()`
  or Jackson row-to-JSON encoder
- Parquet: `FileSink.forBulkFormat(path, ParquetWriterFactory)` using
  `AvroParquetWriters.forReflectRecord(...)` — schema derived from `TargetConfig.schema`
- Rolling: `OnCheckpointRollingPolicy` when `rollOnCheckpoint=true`;
  `DefaultRollingPolicy.withMaxPartSize(maxFileSizeBytes)` otherwise
- Output path template: `{storagePath}/{partitionBy=value}/part-{taskIndex}-{checkpointId}`

### Acceptance tests

- `sink_writesCsvRows_toLocalPath`
- `sink_writesJsonRows_toLocalPath`
- `sink_writesParquetRows_toLocalPath`
- `sink_throwsIllegalArgument_whenStoragePathIsNull`
- `sink_rollsFile_onCheckpoint_whenRollOnCheckpointEnabled`
- `sink_configuresAdlsCredentials_fromStorageConfig`
- `sink_configuresS3Credentials_fromStorageConfig`

---

## Feature 010-C — API / REST Sink (`ApiTargetLayer`) — Hot zone extension

### What

POST each output row as JSON to a REST endpoint with retry, backoff, and DLQ routing.
Supports all four auth mechanisms from `ApiAuthConfig` (shared with 009-C).

### Classes to create

| Class | Package | Role |
|-------|---------|------|
| `ApiTargetLayer` | `sink` | Implements `TargetLayer`; `getSinkType()` → `"API"` |
| `HttpRowSinkFunction` | `sink` | `RichSinkFunction<Row>` — serialises Row to JSON, POSTs, retries |
| `ApiTargetLayerTest` | `sink` (test) | Happy path, retry on 5xx, DLQ on 4xx, each auth type |

### `TargetConfig` additions

```yaml
streaming.job.target:
  type: API
  url: https://api.example.com/ingest
  method: POST
  batchSize: 1                        # 1 = per-record; >1 = batch JSON array POST
  connectTimeoutMs: 5000
  readTimeoutMs: 10000
  retryAttempts: 3
  retryBackoffMs: 500                 # exponential: 500, 1000, 2000 ms
  apiAuth:
    type: BEARER
    token: ${API_BEARER_TOKEN}
  dlq:
    enabled: true
    topic: api-sink-dlq
    bootstrapServers: kafka:29092
```

### Acceptance tests

- `sink_postsRow_asJson_toEndpoint`
- `sink_postsRowBatch_asJsonArray_whenBatchSizeGt1`
- `sink_retries_on5xxResponse_withExponentialBackoff`
- `sink_routesToDlq_on4xxAfterAllRetries`
- `sink_authenticates_withBearer`
- `sink_authenticates_withOauth2`
- `sink_authenticates_withMtls`
- `sink_authenticates_withApiKey`

---

## Feature 010 — Orchestrator / Controller changes

`TargetLayer` already has `getSinkType()`. Inject all `TargetLayer` beans as a list and resolve by type:

```java
@Autowired
public StreamingJobOrchestrator(List<TargetLayer> targetLayers, ...) {
    this.targetLayerMap = targetLayers.stream()
        .collect(Collectors.toMap(t -> t.getSinkType().toUpperCase(), t -> t));
}

TargetLayer resolveTargetLayer(TargetConfig config) {
    String type = config.getType() == null ? "KAFKA" : config.getType().toUpperCase();
    TargetLayer layer = targetLayerMap.get(type);
    if (layer == null) throw new IllegalArgumentException("No TargetLayer for type: " + type);
    return layer;
}
```

`JobController.validateJob()` additions per sink type:
- `JDBC`: `jdbcUrl` non-blank, `tableName` non-blank, upsert key present if `upsertMode=true`
- `FILE`: `storagePath` non-blank, `fileFormat` in allowed set
- `API`: `url` valid URI, `apiAuth.type` set, required auth fields present per type

---

## Feature 011 — Schema Registry Integration (SASL-secured)

### What

Fetch Avro schema from a SASL-secured Confluent Schema Registry.
`KafkaSourceLayer` checks `SchemaConfig.type: REGISTRY` and delegates to
`CachedSchemaRegistryClient` instead of the inline path.

### SASL mechanisms supported

| Mechanism | Config |
|-----------|--------|
| `PLAIN` | `username` + `password` → `Authorization: Basic base64(user:pass)` |
| `SCRAM-SHA-256` | `username` + `password` + `saslMechanism: SCRAM-SHA-256` |
| `SCRAM-SHA-512` | `username` + `password` + `saslMechanism: SCRAM-SHA-512` |

All mechanisms optionally combined with TLS (truststore for CA certificate).

### Classes to create

| Class | Package | Role |
|-------|---------|------|
| `SchemaRegistryClient` | `source` | HTTP client fetching schema by subject/version from registry REST API |
| `CachedSchemaRegistryClient` | `source` | TTL cache wrapping `SchemaRegistryClient`; invalidates on 404 |
| `SchemaRegistryConfig` | `config` | All registry fields (see YAML below) |
| `SchemaRegistryClientTest` | `source` (test) | PLAIN auth, SCRAM auth, TLS, 404, cache TTL |

### `SchemaConfig` additions

```yaml
streaming.job.sources[0]:
  schemaDefinition:
    type: REGISTRY               # INLINE (existing) | REGISTRY (new)
    registryUrl: https://schema-registry:8081
    subject: orders-value        # defaults to <topic>-value if omitted
    version: latest              # specific version number or 'latest'
    cacheTtlMs: 300000           # 5 minutes
    saslMechanism: PLAIN         # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
    username: ${SR_USERNAME}
    password: ${SR_PASSWORD}
    tls:
      enabled: true
      truststorePath: /app/certs/truststore.jks
      truststorePassword: ${SR_TRUSTSTORE_PASSWORD}
      skipHostnameVerification: false
```

### Key implementation points

- PLAIN auth: `Authorization: Basic base64(username:password)` header on every request
- SCRAM: implemented via custom `javax.security.auth.callback.CallbackHandler` wired into
  `javax.security.sasl.Sasl.createSaslClient(...)` — produces `Authorization: SCRAM-SHA-256 ...`
- TLS: `SSLContext` from truststore; `HttpsURLConnection.setDefaultSSLSocketFactory(...)` scoped
  to the registry client instance (not global)
- Cache invalidation: TTL expiry OR explicit `GET /api/schema/refresh/{jobName}` endpoint
  (calls `CachedSchemaRegistryClient.invalidate(subject)`)
- `SqlValidatorService`: when `SchemaConfig.type == REGISTRY`, calls
  `CachedSchemaRegistryClient.fetchSchema(subject)` to build the `SourceEntry`

### Acceptance tests

- `fetchSchema_returnsAvroSchema_withPlainAuth`
- `fetchSchema_returnsAvroSchema_withScramSha256Auth`
- `fetchSchema_throwsSchemaException_forUnknownSubject`
- `fetchSchema_throwsSchemaException_on401_wrongCredentials`
- `cachedClient_returnsFromCache_onSecondCall`
- `cachedClient_refreshes_afterTtlExpiry`
- `cachedClient_invalidates_on404_subjectDeleted`
- `tlsClient_rejectsUntrustedCertificate_whenSkipVerificationFalse`

---

## Feature 012 — Metrics & Observability

### What

Wire Flink's built-in MetricGroup to a Prometheus reporter so all pipeline metrics
are scraped and displayed in a pre-provisioned Grafana dashboard.
Retention: 30-day TSDB storage, 15 s scrape interval.

### Components to create / modify

| Component | Role |
|-----------|------|
| `FlinkMetricsConfig` fields in `FlinkConfig` | `metricsEnabled`, `prometheusPort` |
| `FLINK_PROPERTIES` in `docker-compose.yml` | Prometheus reporter config injected |
| `monitoring/prometheus.yml` | Scrape targets + 30-day retention config |
| `monitoring/grafana/provisioning/datasources/prometheus.yml` | Auto-provision datasource |
| `monitoring/grafana/provisioning/dashboards/flink-pipeline.json` | Pre-built dashboard |
| `PipelineMetrics` | Metric name constants (prevents typos across layers) |

### `FLINK_PROPERTIES` additions

```
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249
metrics.reporter.prom.filterLabelValueCharacters: false
metrics.latency.interval: 5000
```

### Metrics exposed

| Metric | Flink type | Labels | Description |
|--------|-----------|--------|-------------|
| `flink_pipeline_source_records_read_total` | Counter | `job_name`, `source_table` | Records consumed per source |
| `flink_pipeline_source_records_rejected_total` | Counter | `job_name`, `source_table` | Schema-rejected records |
| `flink_pipeline_transform_records_out_total` | Counter | `job_name` | Records after SQL transform |
| `flink_pipeline_sink_records_written_total` | Counter | `job_name`, `sink_type`, `sink_target` | Records accepted by sink |
| `flink_pipeline_sink_records_failed_total` | Counter | `job_name`, `sink_type` | Sink errors |
| `flink_pipeline_dlq_records_routed_total` | Counter | `job_name`, `source_table`, `error_type` | DLQ-routed records |
| `flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency` | Histogram | built-in | End-to-end latency |
| `flink_taskmanager_job_task_operator_numRecordsInPerSecond` | Gauge | built-in | Throughput in |
| `flink_taskmanager_job_task_operator_numRecordsOutPerSecond` | Gauge | built-in | Throughput out |

### `prometheus.yml`

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: flink-jobmanager
    static_configs:
      - targets: ['jobmanager:9249']
  - job_name: flink-taskmanager
    static_configs:
      - targets: ['taskmanager:9249']
  - job_name: flink-app
    static_configs:
      - targets: ['flink-app:8082']
        labels:
          service: spring-app
```

### `docker-compose.yml` additions

```yaml
prometheus:
  image: prom/prometheus:v2.51.0
  container_name: prometheus
  command:
    - '--config.file=/etc/prometheus/prometheus.yml'
    - '--storage.tsdb.path=/prometheus'
    - '--storage.tsdb.retention.time=30d'
    - '--web.enable-lifecycle'
  volumes:
    - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    - prometheus_data:/prometheus
  ports:
    - "9090:9090"

grafana:
  image: grafana/grafana:10.3.0
  container_name: grafana
  environment:
    GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD:admin}
    GF_USERS_ALLOW_SIGN_UP: "false"
  volumes:
    - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
    - grafana_data:/var/lib/grafana
  ports:
    - "3000:3000"
  depends_on:
    - prometheus

volumes:
  prometheus_data:
  grafana_data:
```

### Grafana dashboard panels (pre-provisioned)

| Panel | Metric | Viz |
|-------|--------|-----|
| Records read / sec | `rate(flink_pipeline_source_records_read_total[1m])` | Time series |
| Rejection rate % | `rate(rejected) / rate(read) * 100` | Time series |
| Sink write / sec | `rate(flink_pipeline_sink_records_written_total[1m])` | Time series |
| Sink failures | `rate(flink_pipeline_sink_records_failed_total[1m])` | Time series + alert |
| DLQ routed / sec | `rate(flink_pipeline_dlq_records_routed_total[1m])` | Time series |
| End-to-end latency p50/p99 | Flink latency histogram | Gauge |
| Active jobs | `flink_jobmanager_numRunningJobs` | Stat |
| Checkpoint duration | `flink_jobmanager_job_lastCheckpointDuration` | Gauge |

### `application.yml` additions

```yaml
streaming:
  job:
    flink:
      metrics-enabled: ${METRICS_ENABLED:true}
      prometheus-port: ${PROMETHEUS_PORT:9249}
```

### Acceptance tests

- `metricsConfig_enabledByDefault`
- `pipelineMetrics_constants_matchPrometheusNamingConvention`
- Integration: `prometheusEndpoint_exposesSourceReadCounter_afterRecordConsumed`

---

## New `pom.xml` Dependencies

| Artifact | Version | Scope | Feature |
|----------|---------|-------|---------|
| `flink-connector-jdbc` | `3.1.2-1.17` | compile | 009-B, 010-A |
| `postgresql` | `42.7.3` | runtime | 009-B, 010-A |
| `mysql-connector-j` | `8.3.0` | runtime | 009-B, 010-A (MySQL) |
| `flink-parquet` | `1.18.0` | compile | 009-A, 010-B |
| `parquet-avro` | `1.13.1` | compile | 009-A, 010-B |
| `hadoop-common` | `3.3.6` | provided | 009-A, 010-B (Parquet FS) |
| `hadoop-azure` | `3.3.6` | compile | 009-A, 010-B (ADLS Gen2) |
| `hadoop-aws` | `3.3.6` | compile | 009-A, 010-B (S3) |
| `aws-java-sdk-s3` | `1.12.x` | compile | 009-A, 010-B |
| `azure-identity` | `1.12.x` | compile | 009-A, 010-B (SP auth) |
| `httpclient` | `4.5.14` | compile | 009-C, 010-C |
| `jsonpath` (Jayway) | `2.9.0` | compile | 009-C |
| `kafka-schema-registry-client` | `7.5.3` | compile | 011 |
| `flink-metrics-prometheus` | `1.18.0` | compile | 012 |

---

## Implementation Order & Dependencies

```
Sprint 1: 009-A (File source) ──┐
          010-A (JDBC sink)   ──┴── shared StoragePathResolver, flink-connector-jdbc

Sprint 2: 009-B (JDBC source) ──┐
          010-B (File sink)   ──┴── ADLS/S3 credentials reuse from Sprint 1

Sprint 3: 009-C (API source)  ──┐
          010-C (API sink)    ──┴── shared ApiAuthConfig, HttpClientFactory, OAuthTokenManager

Sprint 4: 011 (Schema Registry)── depends on 009-C HttpClientFactory for mTLS to registry

Sprint 5: 012 (Metrics)       ── wires metric points across all source/sink layers from 1-4
```

---

## TDD Coverage Targets (per WORK_AGREEMENT)

- **≥ 80%** features ✅ unit-tested at PR merge
- **≥ 1** integration test per new source/sink type pair
- **0** features at 🚧 Stub status at PR merge
- Feature doc `src/docs/features/00X-<name>.md` created **before** coding begins
- `FEATURE_COVERAGE_MATRIX.md` updated at feature completion

---

*Maintained by: DataHonDo Engineering*
*Last updated: 2026-06-05*
