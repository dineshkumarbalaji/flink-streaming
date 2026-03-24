# DataHonDo Flink Streaming Service - Product Documentation (v1.0)

## 1. Overview
The **DataHonDo Flink Streaming Service** is a low-code, dynamic streaming platform designed to simplify the creation, deployment, and management of real-time data pipelines. Built on **Apache Flink** and **Spring Boot**, it allows users to define extract, transform, and load (ETL) logic using standard SQL without writing complex Java/Scala code.

## 2. Technology Stack
*   **Core Engine**: Apache Flink 1.18 (Java 8/11/17 Compatible)
*   **Orchestration**: Spring Boot 2.7.x / 3.x
*   **Messaging**: Apache Kafka (Source & Sink)
*   **Containerization**: Docker & Docker Compose
*   **Frontend**: Vanilla JavaScript + HTML5 (Zero-dependency, light-weight)
*   **Serialization**: Jackson (JSON), Apache Avro

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
*   **Kafka Authentication**: Supports SASL_PLAINTEXT and SASL_SSL.
*   **Mechanisms**: PLAIN and SCRAM-SHA-256 support for secure connectivity.

### 3.4 Advanced Event Time Management
*   **Watermark Generation**: simple configuration to handle out-of-order events.
    *   **Processing Time**: Automatically marks event time based on ingestion.
    *   **Event Time**: Extract timestamps from existing message payloads (JSON/Avro) for accurate historical processing.

### 3.5 Operational Visibility & Management
*   **Pre-flight Validation**: Validates Kafka connectivity, Topic existence, and SQL syntax before deployment.
*   **Configuration Management**: Save and Load job configurations (JSON) to replicate pipelines easily.
*   **Metrics**: Real-time visibility into records consumed/produced via Flink Dashboard.

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
2.  **Access UI**: Open `http://localhost:8080`.
3.  **Define Job**: Connect to Source Kafka, write SQL, and Connect to Target.
4.  **Deploy**: Click "Deploy Job" and monitor in Flink Dashboard (`http://localhost:8081`).
