# DataHonDo Flink Streaming Service - Product Documentation (v1.0)

## 1. Overview
The **DataHonDo Flink Streaming Service** is a low-code, dynamic streaming platform designed to simplify the creation, deployment, and management of real-time data pipelines. Built on **Apache Flink** and **Spring Boot**, it allows users to define extract, transform, and load (ETL) logic using standard SQL without writing complex Java/Scala code.

## 2. Technology Stack
*   **Core Engine**: Apache Flink 1.18 (Java 8/11/17 Compatible)
*   **Orchestration**: Spring Boot 2.7.x / 3.x
*   **Messaging**: Apache Kafka (Source & Sink), REST/API endpoints, JDBC databases, Files (CSV/JSON/Parquet)
*   **Containerization**: Docker & Docker Compose
*   **Frontend**: Vanilla JavaScript + HTML5 (Zero-dependency, light-weight)
*   **Serialization**: Jackson (JSON), Apache Avro, Confluent Schema Registry (SASL-secured)
*   **Observability**: Prometheus (30-day retention), Grafana (pre-built dashboards)

## 3. Key Features

### 3.1 Dynamic Job Submission
*   **SQL-Driven Transformations**: Users can define business logic using Flink SQL (e.g., `SELECT * FROM source WHERE amount > 100`).
*   **No Recompilation**: Submit new jobs instantly via the UI/API without modifying or rebuilding the JAR.

### 3.2 Multi-Format Data Support
Seamlessly ingest and publish data in various formats. The system handles serialization/deserialization automatically.
*   **JSON**: Full support with schema validation (JSON Schema).
*   **Avro**: Binary Avro support with user-provided schemas. Automatically converts timestamps/logic types.
*   **String**: Raw text processing for simple logs or unstructured data.

### 3.3 Enterprise Security
*   **Kafka Authentication**: Supports SASL_PLAINTEXT and SASL_SSL with PLAIN and SCRAM-SHA-256.
*   **API Authentication**: Bearer token, OAuth2 client credentials (auto token refresh), mutual TLS (mTLS), and API key (header or query parameter) for REST source and sink endpoints.
*   **Schema Registry Authentication**: SASL-secured Confluent Schema Registry with PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512, plus optional TLS truststore.

### 3.4 Advanced Event Time Management
*   **Watermark Generation**: simple configuration to handle out-of-order events.
    *   **Processing Time**: Automatically marks event time based on ingestion.
    *   **Event Time**: Extract timestamps from existing message payloads (JSON/Avro) for accurate historical processing.

### 3.5 Operational Visibility & Management
*   **Pre-flight Validation**: Validates Kafka connectivity, topic existence (all sources + target), SQL syntax across all source tables (including multi-source JOINs), and savepoint path format/existence before deployment. Both the `/validate` and `/submit` endpoints enforce these checks independently, so direct API calls are equally safe.
*   **Savepoint Path Validation**: Savepoint restore paths are validated for correct URI format and local directory existence before the job is submitted, preventing silent failures inside the Flink runtime.
*   **Configuration Management**: Save and Load job configurations (JSON) to replicate pipelines easily.
*   **Metrics**: Real-time visibility into records consumed/produced via Flink Dashboard.

### 3.6 Audit & Reconciliation
*   **Event Tracking**: Granular tracking of source consumption, SQL transformation outputs, and target delivery rates to ensure strong data consistency pipelines.
*   **Generic Auditing Sinks**: Emits audit telemetry and discrepancy reports flexibly to File System/Console logs, dedicated Kafka Topics, or JDBC databases.
*   **Discrepancy Reporting**: Dynamically identifies un-reconciled jobs allowing threshold alerting against schema failure tolerances and write-lag.
*   **Actual Execution Window**: Reconciliation reports display the true elapsed execution time (e.g., `"2m 34s"`, `"1h 15m"`) rather than the configured checkpoint interval, giving an accurate picture of the processing window.
*   **Eviction Warnings**: When the in-memory audit cache evicts events due to bounded capacity, a `WARN` log is emitted with the running eviction count and a recommendation to enable a persistent sink (JDBC/KAFKA) to retain full history.

### 3.7 Stateful Checkpointing
*   **Configurable Storage Engine**: Jobs seamlessly map states to `HashMapStateBackend` with user-provided checkpoint directories.
*   **Recovery and Backpressure**: Employs strictly bounded watermarks with custom directory URIs enabling exact state recovery on intermittent restarts.

### 3.8 Multi-Source / Multi-Sink Architecture
*   **Source types**: `KAFKA` (event streaming), `FILE` (CSV/JSON/Parquet from local filesystem, Azure ADLS Gen2, or AWS S3), `JDBC` (PostgreSQL, MySQL, Oracle snapshot reads), `API` (REST endpoint polling with at-least-once delivery).
*   **Sink types**: `KAFKA` (event streaming), `JDBC` (warm zone — upsert-capable relational writes), `FILE` (cold zone — CSV/JSON/Parquet with checkpoint-based rolling), `API` (hot zone — REST push with retry and DLQ).
*   **Pluggable dispatch**: The orchestrator selects the correct source/sink layer at job-submit time via the `type` field in `SourceConfig` / `TargetConfig`. Adding a new layer requires no orchestrator code changes.

### 3.9 Observability
*   **Prometheus**: Metrics scraped from Flink JobManager and TaskManager every 15 seconds with 30-day TSDB retention. Access at `http://localhost:9090`.
*   **Grafana**: Pre-provisioned 8-panel dashboard covering throughput, schema rejections, DLQ routing, sink failures, end-to-end latency (p50/p99), checkpoint duration, and running job count. Access at `http://localhost:3000`.

## 4. Use Cases

### 4.1 Real-Time Data Filtration
**Problem**: A payment topic contains millions of transactions, but the fraud team only needs high-value transactions (> $10k).
**Solution**:
*   **Source**: `payments` (JSON)
*   **SQL**: `SELECT * FROM payments WHERE amount > 10000`
*   **Target**: `high-value-payments`
**Outcome**: Reduces downstream processing load by filtering data at the source.

### 4.2 Format Conversion (Modernization)
**Problem**: Legacy systems consume JSON, but the new data lake requires efficient Avro binaries.
**Solution**:
*   **Source**: `legacy-app-logs` (JSON)
*   **Target**: `data-lake-raw` (Avro)
*   **Target Schema**: Provide the Avro schema in the UI.
**Outcome**: Automatic conversion of JSON structure to optimized Avro binary format in real-time.

### 4.3 PII Masking / Transformation
**Problem**: Customer data must be scrubbed of credit card numbers before analytics.
**Solution**:
*   **SQL**: `SELECT user_id, MASK(credit_card), timestamp FROM users`
*   **Target**: `CleanedUsers`
**Outcome**: Compliant real-time data stream available for analytics teams.

## 5. Getting Started
1.  **Start Services**: Run `start_app.bat` (Windows) or `docker-compose up -d`.
2.  **Access UI**: Open `http://localhost:8082`.
3.  **Define Job**: Connect to Source (Kafka / File / JDBC / API), write SQL, and connect to Target.
4.  **Deploy**: Click "Deploy Job" and monitor in Flink Dashboard (`http://localhost:8081`).
5.  **Monitor**: View metrics in Grafana (`http://localhost:3000`) and raw Prometheus data (`http://localhost:9090`).

| Service | URL | Notes |
|---------|-----|-------|
| Flink Streaming App | `http://localhost:8082` | Job submit UI + REST API |
| Flink Dashboard | `http://localhost:8081` | Operator graph, task metrics |
| Kafka UI | `http://localhost:8090` | Topic browser, consumer lag |
| Grafana | `http://localhost:3000` | Pipeline dashboards (admin/admin default) |
| Prometheus | `http://localhost:9090` | Raw metrics query |
| PostgreSQL | `localhost:5432` | Audit/reconciliation tables |

---

## 6. Changelog

### v1.0 — Feature 007: Validation & Error Handling Hardening (2026-06-02)

| Area | Change |
|------|--------|
| `/submit` endpoint | Validates Kafka source and target topic existence before job submission |
| `/validate` + `/submit` | Savepoint path is validated for URI format and local existence |
| SQL validation | All source tables registered in the validator — multi-source JOINs fully checked |
| Reconciliation report | `windowLabel` now reflects actual elapsed execution time, not checkpoint interval |
| Audit cache | WARN log emitted when events or jobs are evicted; running eviction counters exposed |
| Error logging | Flink 1.18 REST client parse errors logged with full exception chain and root cause |

See [src/docs/features/007-validation-and-error-handling-fixes.md](src/docs/features/007-validation-and-error-handling-fixes.md) for full details.

### v1.0 — Feature 008: Job Audit Table, Dashboard & Infrastructure (2026-06-02)

| Area | Change |
|------|--------|
| Job Audit Table | Persistent `job_audit_records` table (JPA) records every submission — status, parallelism, config snapshot, timestamps |
| Dashboard REST API | `GET /api/dashboard/jobs`, `GET /api/dashboard/jobs/{id}`, `POST /api/dashboard/jobs/{id}/stop`, `DELETE /api/dashboard/jobs/{id}` |
| Job History UI | Dashboard tab shows live job history with colour-coded status badges, stop/delete controls, 30 s auto-refresh |
| Status polling | `JobStatusPoller` (@Scheduled every 30 s) tracks live `JobClient` callbacks and updates audit records |
| DLQ routing | Schema-invalid records routed to a configurable dead-letter Kafka topic via Flink side outputs (`OutputTag`) |
| RocksDB backend | `stateBackend: ROCKSDB` config option; falls back to HashMapStateBackend if unavailable |
| Savepoint persistence | `SavepointRegistry` persists history to disk (`<configDir>/<job>-savepoints.json`); survives restarts |
| Configurable config dir | `JOB_CONFIG_DIR` env var / `streaming.job.flink.config-dir` replaces all hardcoded `configs/` paths |

See [src/docs/features/008-job-audit-table-dashboard.md](src/docs/features/008-job-audit-table-dashboard.md) for full details.

### v1.1 — Features 009-012: Multi-Source/Sink, Schema Registry, Monitoring (2026-06-05)

| Area | Change |
|------|--------|
| **Multi-source (009)** | New source types: `FILE` (CSV/JSON/Parquet, Local/ADLS Gen2/S3), `JDBC` (PostgreSQL/MySQL/Oracle snapshot), `API` (REST polling with at-least-once checkpoint) |
| **API authentication (009)** | `ApiAuthConfig` supports BEARER token, OAuth2 client credentials (auto-refresh), mTLS client certificate, and API key (header or query param) |
| **Multi-sink (010)** | New sink types: `JDBC` (upsert-capable warm zone), `FILE` (CSV/JSON/Parquet cold zone with checkpoint rolling), `API` (REST push hot zone with exponential retry + DLQ) |
| **JDBC upsert key** | Resolved from `upsertKeyColumns` config list; falls back to `schema.fields[].primaryKey: true`; dialect-specific SQL for PostgreSQL, MySQL, Oracle, H2 |
| **Storage tiers (009/010)** | `StoragePathResolver` auto-detects URI scheme (Local / `abfs://` ADLS Gen2 / `s3a://` S3) and injects Hadoop FS credentials into the Flink environment |
| **Schema Registry (011)** | `SchemaConfig.type: REGISTRY` fetches Avro schema from a SASL-secured Confluent registry (PLAIN/SCRAM-SHA-256/512 + optional TLS); 5-min TTL cache; 404 auto-invalidation |
| **Prometheus (012)** | Flink `PrometheusReporterFactory` wired; 30-day TSDB retention; 15 s scrape interval; `http://localhost:9090` |
| **Grafana (012)** | Auto-provisioned 8-panel pipeline dashboard; `http://localhost:3000`; covers throughput, rejections, DLQ, sink failures, latency p50/p99, checkpoint duration, running jobs |
| **Orchestrator dispatch** | `StreamingJobOrchestrator` now injects `List<SourceLayer>` + `List<TargetLayer>` and routes by `getSourceType()` / `getSinkType()` — fully backward-compatible with existing `KAFKA` default |

See feature docs [009](src/docs/features/009-multi-source-architecture.md), [010](src/docs/features/010-multi-sink-architecture.md), [011](src/docs/features/011-schema-registry-integration.md), [012](src/docs/features/012-prometheus-grafana-monitoring.md) for full details.
