# DataHonDo Flink Streaming Service

A low-code, dynamic real-time data streaming platform built on **Apache Flink** and **Spring Boot**. Define, deploy, and manage multi-source / multi-sink streaming pipelines using SQL — no Java/Scala coding required.

---

## Prerequisites

| Requirement      | Version / Notes                              |
|------------------|----------------------------------------------|
| Docker Desktop   | Running before starting the app              |
| Java             | 8 or higher                                  |
| Maven            | For building outside Docker                  |
| Git              | To clone the repository                      |

---

## Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/dineshkumarbalaji/flink-streaming.git
cd flink-streaming
```

### 2. Start the application (Windows)
```bat
start_app.bat
```
This builds the Docker image and starts all services automatically.

### 3. Access the UI
Open your browser and go to: **http://localhost:8082**

---

## Service URLs

| Service              | URL                                      | Notes |
|----------------------|------------------------------------------|-------|
| Flink Control App    | http://localhost:8082                    | Job submit UI + REST API |
| Flink Control API    | http://localhost:8082/api/jobs/list      | Running jobs |
| Flink Dashboard      | http://localhost:8081                    | Operator graph, task metrics |
| Kafka UI             | http://localhost:8090                    | Topic browser, consumer lag |
| Grafana              | http://localhost:3000                    | Pipeline dashboards (admin/admin) |
| Prometheus           | http://localhost:9090                    | Raw metrics query |
| Kafka Broker         | localhost:9092                           | |
| ZooKeeper            | localhost:2181                           | |
| PostgreSQL           | localhost:5432                           | Audit / reconciliation tables |

---

## Using the Application

1. **Open the UI** at `http://localhost:8082`
2. **Configure Source** — choose source type (`KAFKA`, `FILE`, `JDBC`, or `API`) and supply connection details
3. **Write SQL** — filter or transform data (e.g. `SELECT * FROM source WHERE amount > 1000`)
4. **Configure Target** — choose sink type (`KAFKA`, `JDBC`, `FILE`, or `API`) and supply connection details
5. **Deploy Job** — click "Deploy Job" and monitor progress on the Flink Dashboard
6. **Monitor** — view real-time metrics in Grafana (`http://localhost:3000`)

---

## Supported Data Formats

| Format | Description                                          |
|--------|------------------------------------------------------|
| JSON   | Full support with optional JSON Schema validation    |
| Avro   | Binary Avro with user-provided schema                |
| String | Raw text / unstructured data                         |

---

## Key Features

| Feature | Description |
|---------|-------------|
| **Multi-source ingestion** | `KAFKA` (event streaming), `FILE` (CSV/JSON/Parquet, Local/ADLS Gen2/S3), `JDBC` (PostgreSQL/MySQL/Oracle), `API` (REST polling) |
| **Multi-sink output** | `KAFKA` (hot), `JDBC` (warm — upsert-capable), `FILE` (cold — Parquet/checkpoint rolling), `API` (REST push with retry) |
| **API authentication** | Bearer token, OAuth2 client credentials (auto token refresh), mTLS, API key (header or query param) |
| **Schema Registry** | Fetch Avro schemas from SASL-secured Confluent Schema Registry (PLAIN/SCRAM); TTL cache; TLS truststore |
| **Multi-source SQL** | JOIN across multiple sources (any type) in a single SQL query |
| **Schema validation** | JSON Schema and Avro schema validation at source ingestion; DLQ side-output for rejected records |
| **Audit & Reconciliation** | Per-run record counting with discrepancy reporting (LOG / KAFKA / JDBC sinks) |
| **Savepoint support** | Trigger, list, and restore from savepoints without stopping the job |
| **Pre-flight validation** | Source connectivity, topic/path existence, SQL syntax, savepoint path — validated before submission |
| **Watermark support** | `PROCESS_TIME` or event-time (`EXISTING` column) watermark strategies |
| **Monitoring** | Prometheus (30-day retention) + pre-built Grafana dashboard: throughput, latency, DLQ, checkpoint, errors |
| **Job Audit Table** | Every submission persisted in `job_audit_records`; REST API + live Job History UI panel |

---

## Authentication

### Kafka
Supports secured Kafka clusters via **SASL_PLAINTEXT** / **SASL_SSL** with `PLAIN` and `SCRAM-SHA-256` mechanisms.

### REST API (source & sink)
| Mechanism | Config |
|-----------|--------|
| Bearer token | `apiAuth.type: BEARER`, `apiAuth.token` |
| OAuth2 (auto-refresh) | `apiAuth.type: OAUTH2`, `tokenUrl`, `clientId`, `clientSecret` |
| Mutual TLS | `apiAuth.type: MTLS`, `keystorePath`, `truststorePath` |
| API key | `apiAuth.type: API_KEY`, `apiKey`, `apiKeyHeader`, `apiKeyLocation` (HEADER or QUERY) |

### Schema Registry
SASL-secured Confluent Schema Registry — `saslMechanism: PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512` with optional TLS truststore.

---

## Scripts

| Script            | Purpose                                         |
|-------------------|-------------------------------------------------|
| `start_app.bat`   | Build and start all services                    |
| `stop_app.bat`    | Stop and remove all containers                  |
| `rebuild_app.bat` | Rebuild and restart the flink-app only          |

---

## Project Structure

```
flink-streaming/
├── src/
│   ├── main/
│   │   ├── java/com/datahondo/flink/streaming/
│   │   │   ├── audit/           # Audit events, reconciliation, in-memory cache, sinks
│   │   │   ├── config/          # Job, Kafka, Flink, Watermark, Audit configs
│   │   │   ├── exception/       # Custom exceptions
│   │   │   ├── job/             # Job orchestration (StreamingJobOrchestrator)
│   │   │   ├── savepoint/       # Savepoint trigger, registry, Flink REST client
│   │   │   ├── source/          # Source layers: Kafka, File, JDBC, API + Schema Registry
│   │   │   ├── sink/            # Sink layers: Kafka, JDBC, File, API
│   │   │   ├── transformation/  # SQL transformation layer
│   │   │   └── web/             # REST API controllers, validators & models
│   │   └── resources/
│   │       └── static/          # Frontend UI (HTML + JS)
│   ├── test/                    # Unit & integration tests (22 test classes)
│   └── docs/
│       ├── WORK_AGREEMENT.md    # Engineering standards and TDD workflow
│       ├── FEATURE_COVERAGE_MATRIX.md
│       ├── features/            # Per-feature functional docs (001–012)
│       └── technical/           # Technical design documents
├── monitoring/                  # Prometheus config + Grafana provisioning + dashboards
├── docker-compose.yml           # Full stack service definitions (incl. Prometheus + Grafana)
├── Dockerfile                   # flink-app container build
├── pom.xml                      # Maven build configuration
├── start_app.bat                # Start script (Windows)
├── stop_app.bat                 # Stop script (Windows)
└── PRODUCT_DOC_v1.0.md          # Full product documentation
```

---

## Tech Stack

- **Apache Flink** 1.18.0 (Java 8)
- **Spring Boot** 2.7.17
- **Apache Kafka** (Confluent 7.5.0)
- **Docker & Docker Compose**
- **Jackson** (JSON), **Apache Avro**, **Confluent Schema Registry**
- **Apache HttpClient** 4.5 (API source/sink, OAuth2, mTLS)
- **Prometheus** v2.51 + **Grafana** 10.3 (Metrics & Observability)
- **PostgreSQL** 15 (Audit, Reconciliation, Job Audit tables)
- **Vanilla JS + HTML5** (Frontend)

---

## Full Documentation

See [PRODUCT_DOC_v1.0.md](PRODUCT_DOC_v1.0.md) for detailed feature descriptions, use cases, and run notes.
