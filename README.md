# DataHonDo Flink Streaming Service

A low-code, dynamic real-time data streaming platform built on **Apache Flink** and **Spring Boot**. Define, deploy, and manage Kafka-to-Kafka streaming pipelines using SQL — no Java/Scala coding required.

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

| Service              | URL                                      |
|----------------------|------------------------------------------|
| Flink Control App    | http://localhost:8082                    |
| Flink Control API    | http://localhost:8082/api/jobs/list      |
| Flink Dashboard      | http://localhost:8081                    |
| Kafka UI             | http://localhost:8090                    |
| Kafka Broker         | localhost:9092                           |
| ZooKeeper            | localhost:2181                           |

---

## Using the Application

1. **Open the UI** at `http://localhost:8082`
2. **Configure Source** — enter your Kafka broker, topic, and message format (JSON / Avro / String)
3. **Write SQL** — filter or transform data (e.g. `SELECT * FROM source WHERE amount > 1000`)
4. **Configure Target** — enter the output Kafka broker and topic
5. **Deploy Job** — click "Deploy Job" and monitor progress on the Flink Dashboard

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
| Multi-source SQL | JOIN across multiple Kafka sources in a single SQL query |
| Schema validation | JSON Schema and Avro schema validation at source ingestion |
| Audit & Reconciliation | Per-run record counting with discrepancy reporting (LOG / KAFKA / JDBC sinks) |
| Savepoint support | Trigger, list, and restore from savepoints without stopping the job |
| Pre-flight validation | Kafka connectivity, topic existence, SQL syntax, and savepoint path — validated before submission |
| Watermark support | `PROCESS_TIME` or event-time (`EXISTING` column) watermark strategies |

---

## Kafka Authentication

Supports secured Kafka clusters via:
- **SASL_PLAINTEXT** / **SASL_SSL**
- Mechanisms: `PLAIN`, `SCRAM-SHA-256`

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
│   │   │   ├── source/          # Kafka source layer with schema validation
│   │   │   ├── target/          # Kafka target/sink layer
│   │   │   ├── transformation/  # SQL transformation layer
│   │   │   └── web/             # REST API controllers, validators & models
│   │   └── resources/
│   │       └── static/          # Frontend UI (HTML + JS)
│   ├── test/                    # Unit & integration tests (22 test classes)
│   └── docs/
│       ├── WORK_AGREEMENT.md    # Engineering standards and TDD workflow
│       ├── FEATURE_COVERAGE_MATRIX.md
│       ├── features/            # Per-feature functional docs (001–007)
│       └── technical/           # Technical design documents
├── docker-compose.yml           # Full stack service definitions
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
- **Jackson** (JSON), **Apache Avro**
- **Vanilla JS + HTML5** (Frontend)

---

## Full Documentation

See [PRODUCT_DOC_v1.0.md](PRODUCT_DOC_v1.0.md) for detailed feature descriptions, use cases, and run notes.
