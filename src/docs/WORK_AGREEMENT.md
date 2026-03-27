# Flink Streaming Work Agreement

## Project: Streaming Framework (Source → Transform → Target)

---

## 1. Objective

This document defines the engineering standards, development workflow, and documentation expectations for building the Flink Streaming Framework using:

- Test-Driven Development (TDD)
- Agent-assisted code generation
- Modular Source → Transform → Target architecture

---

## 2. Architecture Overview

### Core Layers

1. **Source Layer**
   - Kafka / File / API ingestion
   - Schema-driven (Avro / JSON / Registry)

2. **Transform Layer**
   - SQL-based transformations
   - Business logic abstraction
   - Watermark handling (event-time processing)

3. **Target Layer**
   - Kafka / Hive / DB sinks
   - Format: Avro / Parquet / JSON

---

## 3. Development Approach

### Test-Driven Development (TDD)

All features MUST follow:

1. Write Test First
2. Run Test → Fail
3. Generate Code via Agent
4. Run Test → Pass
5. Refactor if needed

### TDD Workflow

```
Requirement → Test Case → Agent Code Generation → Validation → Refactor → Commit
```

---

## 4. Coding Standards

### Java

- Follow standard Java naming conventions (camelCase for methods/fields, PascalCase for classes)
- Use Lombok (`@Data`, `@Builder`, `@Slf4j`, `@RequiredArgsConstructor`) to reduce boilerplate
- All public methods must have Javadoc for non-obvious behavior
- No raw types; use generics where applicable
- Avoid catching generic `Exception` — catch specific exceptions where possible

### Flink-Specific

- Each operator must have a unique `.uid()` assigned for checkpoint recovery
- Use `RichFunction` variants when lifecycle hooks (`open`/`close`) are needed
- Watermark strategies must be explicitly configured — never rely on defaults
- State backends and checkpoint intervals must be configurable via `StreamingJobConfig`

### Configuration

- All job parameters must flow through `StreamingJobConfig` — no hardcoded values in business logic
- Sensitive values (passwords, secrets) must NOT be logged or persisted to disk in plaintext
- Default values must be declared as constants, not inline literals

---

## 4. Agent Usage Guidelines

### Allowed Responsibilities

Agents may be used to:

- Generate boilerplate Flink jobs (source/transform/target wiring)
- Create schema models (POJO / Avro)
- Build SQL transformations
- Generate unit and integration tests

### Restrictions

- No direct production code without test validation — all agent-generated code must pass tests before merge
- No hardcoded schema — all schema definitions must be config-driven via `StreamingJobConfig` or external schema files
- No skipping the validation phase — the TDD cycle (write test → fail → generate → pass) is mandatory

### Agent-Generated Code Review Checklist

Before accepting agent output, verify:

- [ ] Test exists and was written before the code was generated
- [ ] No secrets, passwords, or credentials are hardcoded
- [ ] All Flink operators have unique `.uid()` assignments
- [ ] Schema is externalized (not inline in code)
- [ ] No raw types or unchecked casts introduced
- [ ] Error handling does not silently swallow exceptions

---

## 5. Layer Contracts

### Source Layer (`KafkaSourceLayer`)

| Responsibility | Requirement |
|---|---|
| Read from Kafka | Use `KafkaSource` connector with configurable offset strategy |
| Schema Validation | Validate against JSON Schema or Avro Schema when schema is provided |
| Metrics | Register `records-consumed` counter per source operator |
| Table Registration | Register each source as a named Flink temporary view |
| Watermark | Support `NONE`, `PROCESS_TIME`, and `EXISTING` column modes |

### Transform Layer (`TransformationLayer`)

| Responsibility | Requirement |
|---|---|
| SQL Execution | Execute user-supplied SQL via Flink Table API |
| SQL Source | Accept inline SQL content or file path |
| Result Registration | Register result as a named temporary view |

### Target Layer (`KafkaTargetLayer`)

| Responsibility | Requirement |
|---|---|
| Write to Kafka | Use `KafkaSink` connector |
| Serialization | Support STRING, JSON (proper object), and Avro binary formats |
| Schema-Driven Avro | Map Row fields to Avro schema fields by name, not by position |
| Authentication | Support SASL_SSL and SASL_PLAINTEXT with PLAIN and SCRAM-SHA-256 mechanisms |

---

## 6. Testing Standards

### Unit Tests

- All schema parsing logic must have unit tests with valid and invalid inputs
- All type mapping functions (`mapToDataType`, `createRowTypeInfo`) must be tested for each supported type
- Watermark strategy selection must be tested for each strategy mode

### Integration Tests

- End-to-end job submission tests must use an embedded Kafka (e.g., Testcontainers)
- SQL transformation tests must run against a real `StreamTableEnvironment` (not mocked)
- Checkpoint configuration tests must verify that operators receive the correct settings

### Test Naming Convention

```
methodName_stateUnderTest_expectedBehavior
```

Example: `createSourceTable_withAvroSchema_registersTypedView`

---

## 7. Git Workflow

### Branch Strategy

- `master` — stable, production-ready code only
- `feature/<ticket-id>-<short-description>` — new features
- `fix/<ticket-id>-<short-description>` — bug fixes

### Commit Messages

Format: `<type>: <short summary>`

| Type | Usage |
|---|---|
| `feat` | New feature |
| `fix` | Bug fix |
| `refactor` | Code restructuring without behavior change |
| `test` | Test additions or changes |
| `docs` | Documentation only |
| `chore` | Build, config, tooling changes |

Example: `feat: add AVRO schema validation to KafkaSourceLayer`

### Pull Request Rules

- At least one reviewer approval required before merge
- All CI checks must pass
- PR description must reference the relevant ticket and describe what changed and why

---

## 8. Known Issues and Technical Debt

The following issues are tracked and must be resolved before production release:

| Priority | Component | Issue |
|---|---|---|
| High | `KafkaTargetLayer` | JSON serialization wraps Row.toString() — must produce proper JSON object |
| High | `JobController` | Source passwords are saved in plaintext to config files on disk |
| High | `JobController` | NullPointerException risk when watermark is not configured on first source |
| Medium | `KafkaSourceLayer` | Avro LONG type is downcast to INT — precision loss for large values |
| Medium | `KafkaSourceLayer` | Watermark timestamp assignor uses wall clock even when EXISTING column mode is set |
| Medium | `JobController` | `JobClient` from job submission is discarded — no lifecycle management |
| Low | `StreamingJobOrchestrator` | Duplicate log statement for "Layer 1: Source Layer" |

---

## 9. Functional Documentation

Every feature merged into this project MUST include a functional documentation file placed in `src/docs/features/`.

### Requirements

- One file per feature
- File name format: `<feature-id>-<short-description>.md` (e.g., `001-avro-schema-validation.md`)
- Must be written before or alongside the implementation (not after)
- Must be reviewed as part of the pull request

### Feature Documentation Template

```markdown
# Feature Name

## Business Objective
Explain what business problem this feature solves and why it is needed.

## Input
- **Source type**: Kafka / File / API
- **Topic / Path**: e.g., `orders-raw`
- **Format**: JSON / Avro / String
- **Schema definition**:
  ```json
  {
    "type": "object",
    "properties": {
      "field_name": { "type": "string" }
    }
  }
  ```

## Processing Logic
- **SQL / transformation rules**: describe or include the SQL query
- **Watermark logic**: strategy used (NONE / BOUNDED), timestamp column, max out-of-orderness
- **Filtering / enrichment**: any additional business rules applied

## Output
- **Target system**: Kafka / Hive / DB
- **Topic / Table**: e.g., `orders-enriched`
- **Format**: JSON / Avro / String
- **Output schema**:
  ```json
  {
    "type": "object",
    "properties": {
      "output_field": { "type": "string" }
    }
  }
  ```

## Edge Cases

| Scenario | Handling Strategy |
|---|---|
| Null field values | Set field to null; downstream must handle nullable fields |
| Late events | Dropped if beyond watermark threshold; log warning |
| Duplicate records | Not deduplicated by default; document if idempotency is required |
| Malformed / invalid schema | Record is skipped; error logged with raw value |
| Schema evolution | Not supported without redeployment; document breaking changes |

## Test Coverage
- Unit test file: `src/test/java/.../FeatureNameTest.java`
- Integration test file: `src/test/java/.../FeatureNameIT.java`
- Key scenarios covered: list the main test cases
```

---

## 10. Technical Documentation

Every feature merged into this project MUST include a technical design document placed in `src/docs/technical/`.

### Requirements

- One file per feature or significant component change
- File name format: `<feature-id>-technical-design.md` (e.g., `001-avro-schema-validation-technical-design.md`)
- Must be approved by a technical reviewer before implementation begins
- Must be kept up to date if implementation deviates from the original design

### Technical Design Template

```markdown
# Technical Design: [Feature Name]

## Components

| Component | Class | Responsibility |
|---|---|---|
| Source Connector | `KafkaSourceLayer` | Reads raw events from Kafka topic |
| Transformation Engine | `TransformationLayer` | Applies SQL logic via Flink Table API |
| Sink Connector | `KafkaTargetLayer` | Serializes and writes results to Kafka |

## Data Flow

Describe the end-to-end pipeline from ingestion to output:

```
[Kafka Topic] → KafkaSource (offset: EARLIEST)
  → SchemaValidator (JSON/Avro)
  → DataStream<Row>
  → StreamTableEnvironment (SQL transformation)
  → DataStream<Row>
  → Serializer (JSON/Avro/String)
  → KafkaSink [Output Topic]
```

Include operator UIDs, parallelism boundaries, and any side outputs.

## Schema Handling

- **Type**: Static / Dynamic
- **Source**: File-based (`src/resources/schemas/`) / Registry (Schema Registry URL)
- **Formats supported**: JSON Schema (Draft 2019-09) / Avro IDL
- **Validation point**: Applied at source ingestion before entering the Table API
- **Schema evolution**: describe forward/backward compatibility requirements

## Configuration

List all `StreamingJobConfig` properties used by this feature:

| Property | Type | Default | Description |
|---|---|---|---|
| `streaming.job.jobName` | String | — | Unique job identifier |
| `streaming.job.flink.parallelism` | Integer | 1 | Operator parallelism |
| `streaming.job.flink.checkpointInterval` | Long | 60000 | Checkpoint interval in ms |
| `streaming.job.flink.checkpointDir` | String | null | Checkpoint storage path |
| `streaming.job.sources[].kafka.topic` | String | — | Source Kafka topic |
| `streaming.job.sources[].kafka.format` | String | STRING | Message format (JSON/AVRO/STRING) |
| `streaming.job.sources[].schema` | String | null | Schema definition (JSON or Avro) |
| `streaming.job.target.kafka.topic` | String | — | Sink Kafka topic |
| `streaming.job.target.kafka.format` | String | STRING | Output format |

## Error Handling

| Error Type | Strategy |
|---|---|
| Malformed JSON / Avro | Skip record; log raw value at ERROR level |
| Schema validation failure | Skip record; log field-level validation errors |
| Kafka source unavailable | Flink retries with exponential backoff (connector default) |
| Kafka sink write failure | Flink retries; job fails after max retry exhaustion |
| SQL execution error | Job fails immediately; exception propagated to `JobClient` |
| Serialization error (target) | Record dropped and exception logged; rethrow to fail task |

**Dead Letter Queue (DLQ):**
- Not implemented by default
- If required, route invalid records to a separate Kafka topic via Flink side outputs
- DLQ topic naming convention: `<source-topic>-dlq`

## Performance Considerations

### Parallelism
- Default parallelism: configured via `streaming.job.flink.parallelism`
- Source operator parallelism bounded by Kafka partition count
- Transformation and sink operators inherit job-level parallelism

### Checkpointing
- Interval: configured via `streaming.job.flink.checkpointInterval`
- Storage: `HashMapStateBackend` with configurable checkpoint directory
- Max concurrent checkpoints: configured via `streaming.job.flink.maxConcurrentCheckpoints`
- At-least-once delivery guarantee (Flink default); exactly-once requires transactional Kafka sink

### State Backend
- `HashMapStateBackend` — in-memory, suitable for low-to-medium state sizes
- For large state (e.g., windowed aggregations), consider `EmbeddedRocksDBStateBackend`
- Checkpoint storage path must be accessible by all TaskManagers (e.g., shared HDFS or S3)

### Backpressure
- Monitor via Flink Web UI backpressure tab
- If source outpaces sink, increase sink parallelism or optimize serialization
```

---

## 11. Testing Standards

All code in this project must meet the testing requirements below before it is considered mergeable.

---

### Unit Testing

**Scope:** Test individual classes and methods in isolation, without a running Flink cluster or Kafka broker.

**What must be unit tested:**

| Area | Examples |
|---|---|
| Transformation logic | SQL result correctness, field mapping, aggregation rules |
| Schema validation | Valid JSON/Avro accepted, invalid records rejected, null fields handled |
| Utility functions | `parseSchemaString`, `parseAvroSchemaString`, `mapToDataType`, `createRowTypeInfo` |
| Offset strategy selection | Each `startingOffset` value maps to the correct `OffsetsInitializer` |
| Watermark configuration | Each watermark mode produces the correct `WatermarkStrategy` |
| Serialization | JSON, Avro, and String output produce correct byte representation |
| Config mapping | `JobRequest` → `StreamingJobConfig` mapping is lossless and correct |

**Rules:**

- Use JUnit 5 (`@Test`, `@ParameterizedTest`) and Mockito for dependencies
- One test class per production class: `KafkaSourceLayerTest`, `TransformationLayerTest`, etc.
- Each test method covers exactly one behavior — no multi-assertion omnibus tests
- Naming convention: `methodName_stateUnderTest_expectedBehavior`

**Example structure:**

```java
@Test
void parseSchemaString_withIntegerField_mapsToIntType() { ... }

@Test
void parseSchemaString_withMissingTypeNode_defaultsToString() { ... }

@Test
void parseSchemaString_withMalformedJson_throwsSchemaException() { ... }
```

---

### Integration Testing

**Scope:** Test the full pipeline end-to-end — Kafka source through Flink processing to Kafka sink — using real infrastructure via Testcontainers.

**What must be integration tested:**

| Scenario | Validation |
|---|---|
| Kafka → Flink → Sink (happy path) | Records consumed from source topic appear correctly in target topic |
| JSON schema validation | Invalid records are dropped; valid records pass through |
| Avro schema validation | Avro-encoded records are deserialized and re-serialized correctly |
| SQL transformation | Transformed field values in sink match expected SQL output |
| Watermark with late events | Late events beyond threshold are dropped; on-time events pass |
| Multi-source join | Records from two Kafka topics are joined correctly via SQL |
| Checkpoint recovery | Job restarts from checkpoint and does not reprocess already-committed records |

**Infrastructure:**

- Use [Testcontainers](https://testcontainers.com/) for Kafka (`KafkaContainer`) and Flink (`GenericContainer` or embedded mini-cluster)
- Flink mini-cluster (`MiniClusterWithClientResource`) is preferred for CI speed
- Each integration test must clean up topics and state after execution

**Rules:**

- Integration test classes must be suffixed `IT`: `KafkaPipelineIT`, `AvroSchemaValidationIT`
- Annotate with `@Tag("integration")` to allow selective test execution
- Do not mock Kafka or the Flink runtime — use real connectors
- Assert on actual bytes consumed from the sink topic, not on intermediate state

**Example structure:**

```java
@Tag("integration")
class KafkaPipelineIT {

    @Test
    void submitJob_withJsonSource_sinksTransformedRecordsToTargetTopic() {
        // Arrange: produce records to source topic
        // Act: submit Flink job via StreamingJobOrchestrator
        // Assert: consume from target topic and verify field values
    }

    @Test
    void submitJob_withInvalidJsonRecord_dropsRecordAndContinues() { ... }
}
```

---

### Test Coverage Targets

| Layer | Unit Coverage | Integration Coverage |
|---|---|---|
| `KafkaSourceLayer` | ≥ 80% | All schema formats + watermark modes |
| `TransformationLayer` | ≥ 90% | SQL with join, filter, aggregation |
| `KafkaTargetLayer` | ≥ 80% | All output formats (JSON, Avro, String) |
| `JobController` | ≥ 75% | Submit + validate endpoints |
| `StreamingJobOrchestrator` | ≥ 70% | Local and remote env creation |

---

### CI Test Execution

```bash
# Run unit tests only
mvn test

# Run integration tests only
mvn verify -P integration-tests

# Run all tests
mvn verify
```

Integration tests must pass in CI before any PR can be merged.

---

## 12. Error Handling Strategy

All error handling in this project must follow the strategies defined below. Ad-hoc exception swallowing or silent failures are not permitted.

---

### Retry Strategy

Retries apply at two levels: **connector-level** (managed by Flink) and **application-level** (managed by job configuration).

#### Connector-Level Retries

| Connector | Retry Behavior |
|---|---|
| `KafkaSource` | Flink automatically retries on transient broker disconnects using internal reconnect logic |
| `KafkaSink` | Retries on write failure up to `delivery.timeout.ms` (Kafka producer config); job fails after exhaustion |
| Flink Task failure | Flink restarts the failed task according to the configured restart strategy |

#### Application-Level Restart Strategy

Configure via `StreamingJobConfig` or `flink-conf.yaml`:

```yaml
# Fixed delay restart strategy (default)
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10s

# Exponential backoff (recommended for production)
restart-strategy: exponential-delay
restart-strategy.exponential-delay.initial-backoff: 1s
restart-strategy.exponential-delay.max-backoff: 60s
restart-strategy.exponential-delay.multiplier: 2.0
restart-strategy.exponential-delay.max-attempts: 5
```

#### Per-Record Error Handling

Records that fail validation or processing must NOT cause the job to crash. Use the following policy:

| Error Type | Action |
|---|---|
| Schema validation failure | Skip record → route to DLQ → log at ERROR |
| Type conversion failure | Skip record → route to DLQ → log at ERROR |
| Malformed JSON / Avro | Skip record → route to DLQ → log raw value at ERROR |
| Null required field | Skip record → route to DLQ → log field name at WARN |
| Serialization failure (sink) | Skip record → route to DLQ → rethrow if DLQ also fails |

---

### Dead Letter Queue (DLQ)

All unprocessable records must be routed to a dedicated DLQ Kafka topic rather than silently dropped.

#### Naming Convention

```
<source-topic>-dlq
```

Examples: `orders-raw-dlq`, `user-events-dlq`

#### DLQ Record Format

Each DLQ record must be a JSON envelope containing:

```json
{
  "originalPayload": "<raw record as string>",
  "errorType": "SCHEMA_VALIDATION | TYPE_CONVERSION | MALFORMED | SERIALIZATION",
  "errorMessage": "<exception message>",
  "sourcetopic": "<originating Kafka topic>",
  "jobName": "<Flink job name>",
  "timestamp": "<ISO-8601 UTC timestamp>"
}
```

#### Implementation Pattern

Use Flink **side outputs** to route invalid records without blocking the main stream:

```java
// Define DLQ output tag
OutputTag<DlqRecord> dlqTag = new OutputTag<DlqRecord>("dlq") {};

// In FlatMapFunction: route to DLQ instead of dropping
ctx.output(dlqTag, new DlqRecord(rawValue, errorType, errorMessage, ...));

// Attach DLQ sink
processedStream.getSideOutput(dlqTag).sinkTo(dlqKafkaSink);
```

#### DLQ Configuration Properties

| Property | Type | Description |
|---|---|---|
| `streaming.job.dlq.enabled` | Boolean | Enable or disable DLQ routing |
| `streaming.job.dlq.topic` | String | DLQ topic name (defaults to `<source-topic>-dlq`) |
| `streaming.job.dlq.bootstrapServers` | String | Kafka broker for DLQ (can differ from source) |

---

### Alerting Mechanism

Alerts notify the operations team of job degradation or failure without requiring manual log inspection.

#### Alert Triggers

| Condition | Severity | Action |
|---|---|---|
| Flink job enters `FAILED` state | CRITICAL | Immediate alert |
| DLQ record rate exceeds threshold | WARNING | Alert after N records in T seconds |
| Checkpoint failure | WARNING | Alert after 3 consecutive failures |
| Consumer lag exceeds threshold | WARNING | Alert when lag > configured max |
| Task restart limit reached | CRITICAL | Immediate alert |

#### Flink Metrics Integration

Expose the following Flink metrics to the monitoring system (Prometheus / Grafana recommended):

| Metric | Description |
|---|---|
| `records-consumed` | Total records ingested from Kafka source |
| `records-dlq` | Total records routed to DLQ |
| `numRestarts` | Number of job restarts since launch |
| `lastCheckpointDuration` | Duration of the last completed checkpoint |
| `currentInputWatermark` | Current event-time watermark position |

Register custom metrics via `getRuntimeContext().getMetricGroup()` in `RichFunction` operators.

#### Alerting Configuration

```yaml
# Example Prometheus alert rule
- alert: FlinkJobFailed
  expr: flink_jobmanager_job_status == 6   # 6 = FAILED
  for: 0m
  labels:
    severity: critical
  annotations:
    summary: "Flink job has failed"
    description: "Job {{ $labels.job_name }} entered FAILED state"

- alert: HighDlqRate
  expr: rate(flink_taskmanager_job_task_operator_records-dlq[1m]) > 10
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "High DLQ rate detected"
    description: "More than 10 records/sec routed to DLQ on job {{ $labels.job_name }}"
```

#### Notification Channels

| Channel | Use Case |
|---|---|
| Email / Slack | WARNING and CRITICAL alerts |
| PagerDuty / OpsGenie | CRITICAL alerts requiring immediate response |
| Dashboard (Grafana) | Continuous visibility into all metrics |

---

## 13. Governance Rules

These rules are non-negotiable and enforced at code review. Any PR that violates a governance rule must be rejected until resolved.

---

### Rule 1: No Direct Schema Changes Without Versioning

Schema definitions are a contract between producers and consumers. Unversioned changes break downstream systems silently.

**Requirements:**

- Every schema change must increment the schema version
- Schema files must be named with their version: `order-event-v1.avsc`, `order-event-v2.avsc`
- Breaking changes (field removal, type change, rename) require a new major version and a migration plan
- Backward-compatible changes (adding an optional field) require a new minor version
- Schema version must be documented in the feature's functional doc and technical design doc
- Retired schema versions must be archived, not deleted

**Versioning convention:**

```
schemas/
  <entity>-v<major>.<minor>.<patch>.avsc   # Avro
  <entity>-v<major>.<minor>.<patch>.json   # JSON Schema
```

Examples:
```
schemas/order-event-v1.0.0.avsc     # initial
schemas/order-event-v1.1.0.avsc     # added optional field
schemas/order-event-v2.0.0.avsc     # breaking change — requires migration
```

**Change approval:**

| Change Type | Approval Required |
|---|---|
| Patch (documentation, description only) | 1 reviewer |
| Minor (additive, backward-compatible) | 1 reviewer + consumer team notified |
| Major (breaking change) | 2 reviewers + migration plan + consumer team sign-off |

---

### Rule 2: All SQL Must Be Externalized

SQL embedded in Java source code is untestable, unreadable, and impossible to review independently. All SQL must live outside the application binary.

**Requirements:**

- SQL queries must be stored in `src/main/resources/sql/` or provided at runtime via the `TransformationConfig.sqlContent` field from the API
- No SQL string literals in Java source code (except in tests using minimal inline SQL for fixture setup)
- SQL files must be named descriptively: `<feature-id>-<description>.sql`
- Each SQL file must include a header comment block:

```sql
-- Feature:     Order Enrichment
-- Version:     1.0.0
-- Description: Joins raw orders with customer data and filters cancelled orders
-- Author:      <team or agent>
-- Last Updated: YYYY-MM-DD
```

- SQL changes must be reviewed by someone with Flink SQL expertise
- Parameterized values (topic names, table names) must not be hardcoded in SQL — pass via config

**Enforcement:**

```
grep -r "sqlQuery\|tableEnv.sqlQuery" src/main/java --include="*.java"
```

Any match outside of `TransformationLayer.java` is a violation.

---

### Rule 3: No Business Logic in Source or Sink Layers

The Source and Sink layers are infrastructure — they handle I/O, serialization, and schema validation only. Business logic belongs exclusively in the Transform layer.

**What is allowed in Source / Sink layers:**

| Allowed | Not Allowed |
|---|---|
| Reading from Kafka | Filtering records by business criteria |
| Schema validation (structural) | Enriching records with lookup data |
| Type conversion (JSON → Row, Avro → Row) | Applying business rules or thresholds |
| Watermark assignment | Joining streams |
| Writing to Kafka | Aggregating or computing derived fields |
| Serialization (Row → JSON/Avro/bytes) | Conditionally routing based on field values |
| Metrics counting | Any `if` condition on business field values |

**Rationale:** Mixing concerns makes the pipeline impossible to test in isolation and creates hidden coupling between infrastructure and business rules. SQL in `TransformationLayer` is the single authoritative place for all business logic.

**Enforcement at review:**

- Any `if` / `switch` statement in `KafkaSourceLayer` or `KafkaTargetLayer` that branches on a business field value (not a config flag or format type) is a violation
- Any call to external services or databases from source/sink layers is a violation
- Reviewers must explicitly confirm this rule is satisfied in the PR checklist

---

### Governance Enforcement Checklist (PR Review)

Every PR touching pipeline code must verify:

- [ ] Schema version incremented if any schema file changed
- [ ] No SQL string literals in Java source code (outside `TransformationLayer`)
- [ ] SQL file has header comment block with version and description
- [ ] No business logic (field-value branching, enrichment, joins) in `KafkaSourceLayer` or `KafkaTargetLayer`
- [ ] Breaking schema change has consumer team sign-off attached to PR
- [ ] New schema files follow the versioning naming convention
- [ ] SQL changes reviewed by a Flink SQL-qualified reviewer

---

## 14. Configuration-Driven Design

All pipeline behavior must be controlled through configuration — not through code changes. A job should be deployable to a new environment, with a different schema, or against a different topic without touching Java source code.

---

### Core Principle

> **If changing pipeline behavior requires a code change, it is a design violation.**

The only legitimate reasons to change Java code are:
- Adding a new capability (new format, new connector type)
- Fixing a bug
- Performance optimization

Everything else — topics, schemas, SQL, parallelism, auth, offsets — must be config.

---

### Configuration Hierarchy

Configuration is resolved in the following priority order (highest wins):

```
1. API Request (JobRequest payload)         ← runtime, per-job
2. Environment Variables                    ← deployment-time overrides
3. application-<profile>.yml               ← environment-specific defaults
4. application.yml                         ← base defaults
```

---

### Configuration Structure (`StreamingJobConfig`)

```yaml
streaming:
  job:
    jobName: my-streaming-job

    flink:
      remote: true                          # true = connect to Flink cluster; false = local mini-cluster
      host: jobmanager                      # Flink JobManager host
      port: 8081                            # Flink REST port
      jarPath: /app/flink-job.jar           # JAR submitted to remote cluster
      parallelism: 2                        # Default operator parallelism
      checkpointInterval: 60000            # Checkpoint interval in ms
      checkpointDir: s3://bucket/checkpoints
      maxConcurrentCheckpoints: 1

    sources:
      - tableName: orders_raw              # Flink temporary view name
        schema: |                          # JSON Schema or Avro Schema (inline or path)
          { "type": "object", "properties": { "id": { "type": "integer" } } }
        kafka:
          bootstrapServers: kafka:9092
          topic: orders-raw
          groupId: flink-consumer-group
          format: JSON                     # JSON | AVRO | STRING
          startingOffset: EARLIEST         # EARLIEST | LATEST | GROUP_OFFSETS | TIMESTAMP
          startingOffsetTimestamp: null    # Epoch ms — required if startingOffset = TIMESTAMP
          properties:                      # Pass-through Kafka consumer properties
            max.poll.records: 500
          authentication:
            type: SASL_SSL                 # NONE | SASL_SSL | SASL_PLAINTEXT
            mechanism: PLAIN               # PLAIN | SCRAM-SHA-256
            username: ${KAFKA_USERNAME}    # Inject from environment variable
            password: ${KAFKA_PASSWORD}

        watermark:
          strategy: BOUNDED               # NONE | BOUNDED
          mode: EXISTING                  # PROCESS_TIME | EXISTING
          timestampColumn: event_time
          maxOutOfOrderness: 5000         # ms

    transformation:
      sqlContent: null                    # Inline SQL (takes priority over sqlFilePath)
      sqlFilePath: sql/order-enrichment-v1.0.0.sql
      resultTableName: orders_enriched

    target:
      schema: null                        # Avro schema for AVRO output format
      kafka:
        bootstrapServers: kafka:9092
        topic: orders-enriched
        format: JSON                      # JSON | AVRO | STRING
        authentication:
          type: NONE

    dlq:
      enabled: true
      topic: orders-raw-dlq
      bootstrapServers: kafka:9092
```

---

### Rules

**What must be config-driven:**

| Concern | Config Property |
|---|---|
| Kafka topic names | `sources[].kafka.topic`, `target.kafka.topic` |
| Bootstrap servers | `sources[].kafka.bootstrapServers`, `target.kafka.bootstrapServers` |
| Message format | `sources[].kafka.format`, `target.kafka.format` |
| Schema definition | `sources[].schema`, `target.schema` |
| SQL query | `transformation.sqlContent` or `transformation.sqlFilePath` |
| Parallelism | `flink.parallelism` |
| Checkpoint interval | `flink.checkpointInterval` |
| Authentication | `sources[].kafka.authentication`, `target.kafka.authentication` |
| Offset strategy | `sources[].kafka.startingOffset` |
| Watermark behavior | `sources[].watermark.*` |
| DLQ routing | `dlq.enabled`, `dlq.topic` |

**What must NOT be hardcoded in Java:**

- Topic names (even "default" topics)
- Bootstrap server addresses
- Schema field names or types
- SQL query strings
- Parallelism values
- Credential values (use environment variable injection: `${ENV_VAR}`)

---

### Secret Management

Credentials must never appear as plaintext in config files committed to source control.

| Secret | Required Approach |
|---|---|
| Kafka username / password | Environment variable injection (`${KAFKA_USERNAME}`) |
| Schema Registry credentials | Environment variable injection |
| Any API key or token | Environment variable injection or secrets manager (Vault, AWS Secrets Manager) |

Config files committed to git must contain only variable references, never resolved values:

```yaml
# CORRECT
password: ${KAFKA_PASSWORD}

# VIOLATION — never commit
password: myActualPassword123
```

---

## 15. UI-Driven Job Submission Workflow

This section defines the expected end-to-end workflow for submitting a Flink streaming job through the web UI. This is the primary interaction model for the system.

---

### Workflow Overview

```
User fills UI form
       │
       ▼
 [Validate] button
       │
       ├── Source Kafka connection check  (POST /api/jobs/validate)
       ├── Target Kafka connection check
       └── SQL syntax check
              │
         ┌────┴────┐
       FAIL       PASS
         │           │
    Show errors    Enable [Submit] button
                      │
                      ▼
               [Submit] button
                      │
                      ▼        (POST /api/jobs/submit)
              Map form → StreamingJobConfig
                      │
                      ▼
         Save  <jobName>.json  to  configs/
                      │
                      ▼
         Submit Flink job via StreamingJobOrchestrator
                      │
                      ▼
              Job running on Flink cluster
```

---

### Step 1: User Fills the Configuration Form

The UI form (`index.html`) collects all job parameters across four sections:

**General**

| Field | Description | Required |
|---|---|---|
| Job Name | Unique identifier — used as the JSON file name | Yes |
| Parallelism | Number of parallel Flink operators | Yes |
| Checkpoint Interval | Checkpoint frequency in milliseconds | Yes |

**Source(s)** — one or more sources can be added dynamically

| Field | Description | Required |
|---|---|---|
| Bootstrap Servers | Kafka broker address | Yes |
| Topic | Source Kafka topic name | Yes |
| Group ID | Kafka consumer group ID | Yes |
| Format | Message format: `STRING` / `JSON` / `AVRO` | Yes |
| Starting Offset | `EARLIEST` / `LATEST` / `GROUP_OFFSETS` / `TIMESTAMP` | Yes |
| Timestamp | Epoch ms — shown only when offset = `TIMESTAMP` | Conditional |
| Table Name | Flink view name used in SQL | Yes |
| Schema | JSON Schema or Avro Schema definition | Optional |
| Authentication | `NONE` / `SASL_SSL` / `SASL_PLAINTEXT` + credentials | Conditional |
| Enable Watermark | Toggle watermark configuration | Optional |
| Watermark Mode | `PROCESS_TIME` / `EXISTING` | Conditional |
| Timestamp Column | Column name used for event-time watermark | Conditional |

**Transformation**

| Field | Description | Required |
|---|---|---|
| SQL Query | Flink SQL query referencing source table names | Yes |
| Result Table Name | Name for the transformed output view | Yes |

**Target**

| Field | Description | Required |
|---|---|---|
| Bootstrap Servers | Sink Kafka broker address | Yes |
| Topic | Target Kafka topic name | Yes |
| Format | Output format: `STRING` / `JSON` / `AVRO` | Yes |
| Schema | Avro schema for `AVRO` output format | Conditional |
| Authentication | `NONE` / `SASL_SSL` / `SASL_PLAINTEXT` + credentials | Conditional |

---

### Step 2: Validation (`POST /api/jobs/validate`)

The **Validate** button triggers the validation endpoint. The **Submit** button remains disabled until validation passes.

**Validation sequence (`JobController.validateJob`):**

```
1. Check sources are configured (not empty)
2. For each source:
   └── Test Kafka broker connectivity  (KafkaValidatorService)
3. Test target Kafka broker connectivity
4. Validate SQL syntax against first source table/schema  (SqlValidatorService)
```

**Validation response (`ValidationResponse`):**

```json
{
  "valid": true,
  "logs": [
    "Starting validation for job: my-job",
    "Validating Source Configs...",
    "✅ Source 'orders-raw' Connection OK",
    "Validating Target Config...",
    "✅ Target Kafka Connection OK",
    "✅ Target Topic 'orders-enriched' Accessible",
    "Validating SQL Query...",
    "✅ SQL Syntax OK",
    "✅ All checks passed. Ready to deploy."
  ]
}
```

On failure, the log shows the failing step with an `❌` prefix and validation stops. The Submit button stays disabled.

---

### Step 3: Config File Creation (`configs/<jobName>.json`)

On successful submission, the controller saves a structured JSON snapshot of the job configuration to disk via `saveJobConfig()`.

**File location:**
```
configs/<jobName>.json
```

**File structure (`SavedJobConfig`):**

```json
{
  "jobName": "order-enrichment",
  "parallelism": 2,
  "checkpointInterval": 60000,
  "sources": [
    {
      "sourceTopic": "orders-raw",
      "sourceBootstrapServers": "kafka:9092",
      "sourceGroupId": "flink-group",
      "sourceAuthType": "NONE",
      "sourceStartingOffset": "EARLIEST",
      "sourceTableName": "orders_raw",
      "sourceSchema": "{ ... }",
      "sourceFormat": "JSON",
      "enableWatermark": true,
      "watermarkMode": "EXISTING",
      "watermarkColumn": "event_time"
    }
  ],
  "transformation": {
    "sqlQuery": "SELECT id, amount FROM orders_raw WHERE amount > 0",
    "resultTableName": "orders_enriched"
  },
  "target": {
    "targetTopic": "orders-enriched",
    "targetBootstrapServers": "kafka:9092",
    "targetAuthType": "NONE",
    "targetFormat": "JSON"
  }
}
```

This file serves as the **source of truth** for the submitted job and can be used to resubmit or audit the job configuration.

---

### Step 4: Flink Job Submission (`POST /api/jobs/submit`)

After the config file is saved, `StreamingJobOrchestrator.submitJob()` is called:

```
StreamingJobConfig
       │
       ▼
 Create StreamExecutionEnvironment
 (LOCAL or REMOTE based on application.yml flink.remote)
       │
       ▼
 Configure: parallelism, checkpoint interval, state backend
       │
       ▼
 Layer 1 — KafkaSourceLayer
   For each source:
     → Create KafkaSource connector
     → Apply schema validation (if schema provided)
     → Register as Flink temporary view
       │
       ▼
 Layer 2 — TransformationLayer
   → Execute SQL query against registered views
   → Register result as temporary view
       │
       ▼
 Layer 3 — KafkaTargetLayer
   → Serialize result stream (JSON / AVRO / STRING)
   → Attach KafkaSink
       │
       ▼
 env.executeAsync(jobName)  →  Job running
```

---

### UI Interaction Rules

- The **Submit** button must be disabled on page load and re-disabled whenever any form field changes after a successful validation
- Validation must always be re-run after any config change before submission is allowed
- The validation log panel must be cleared and re-populated on each validation run
- Multiple sources can be added dynamically; the first source cannot be removed
- Auth fields are hidden unless auth type is not `NONE`
- Watermark column field is hidden unless watermark mode is `EXISTING`
- Offset timestamp field is hidden unless starting offset is `TIMESTAMP`

---

### Known Workflow Gaps (To Be Resolved)

| Gap | Impact | Priority |
|---|---|---|
| `JobClient` discarded after submit | No way to check job status or cancel from UI | High |
| Config JSON saved to relative path `configs/` | File location unpredictable across environments | High |
| Passwords saved in plaintext to config JSON | Security risk — credentials persisted on disk | High |
| SQL validated only against first source schema | Multi-source SQL may pass validation but fail at runtime | Medium |
| No job list or history view in UI | User cannot see previously submitted jobs | Medium |

---

## 16. Job Audit Table and Dashboard

### Overview

Every submitted Flink job must be recorded in a persistent audit table. The UI dashboard reads from this table to display all known jobs, their live status, and provides controls to start and stop each job. The `JobClient` returned by `env.executeAsync()` is the bridge between the Spring application and the running Flink job — it must no longer be discarded.

---

### Audit Record Model

Each row in the audit table represents one submitted job instance.

| Column | Type | Description |
|---|---|---|
| `id` | Long (PK, auto) | Internal surrogate key |
| `job_name` | String | Human-readable job name from `JobRequest.jobName` |
| `flink_job_id` | String | Flink-assigned `JobID` returned by `JobClient.getJobID()` |
| `yarn_application_id` | String | YARN Application ID (populated when running on YARN cluster; null for local/standalone) |
| `status` | Enum | `SUBMITTING` → `RUNNING` → `FINISHED` / `FAILED` / `CANCELLED` |
| `parallelism` | Integer | Configured parallelism |
| `checkpoint_interval` | Long | Checkpoint interval in ms |
| `config_file_path` | String | Absolute path to `configs/<jobName>.json` |
| `config_snapshot` | Text (JSON) | Full `SavedJobConfig` JSON snapshot at submission time |
| `submitted_at` | Timestamp | When the job was submitted |
| `updated_at` | Timestamp | Last time status was refreshed |
| `error_message` | String | Populated when status is `FAILED`; null otherwise |

**Status lifecycle:**

```
SUBMITTING  ──► RUNNING ──► FINISHED
                   │
                   ├──► FAILED
                   └──► CANCELLED   (via stop action from UI)
```

---

### Storage

Use **Spring Data JPA with H2** (embedded, zero-infrastructure) for development and local runs. For production, swap to PostgreSQL via Spring profile.

**Dependencies to add to `pom.xml`:**

```xml
<!-- Audit Storage -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
</dependency>
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <scope>runtime</scope>
</dependency>
```

**`application.yml` additions:**

```yaml
spring:
  datasource:
    url: jdbc:h2:file:./data/audit-db
    driver-class-name: org.h2.Driver
  jpa:
    hibernate:
      ddl-auto: update
    show-sql: false
```

---

### New Components Required

```
com.datahondo.flink.streaming.audit
  ├── JobAuditRecord.java          @Entity — maps to audit table
  ├── JobAuditRepository.java      @Repository — Spring Data JPA
  ├── JobAuditService.java         @Service — create, update, query audit records
  └── JobStatusPoller.java         @Scheduled — polls Flink REST API to refresh status

com.datahondo.flink.streaming.web
  └── JobDashboardController.java  @RestController — dashboard API endpoints
```

---

### API Endpoints

**Dashboard endpoints (`JobDashboardController`):**

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/jobs` | List all audit records (all jobs, all statuses) |
| `GET` | `/api/jobs/{id}` | Get single audit record by internal ID |
| `GET` | `/api/jobs/{id}/status` | Get live status from Flink REST API |
| `POST` | `/api/jobs/{id}/stop` | Cancel the running Flink job via `JobClient` or Flink REST API |
| `POST` | `/api/jobs/{id}/restart` | Resubmit job using saved `config_snapshot` |
| `DELETE` | `/api/jobs/{id}` | Remove audit record (only allowed when status is `FINISHED` / `CANCELLED` / `FAILED`) |

**List response (`GET /api/jobs`):**

```json
[
  {
    "id": 1,
    "jobName": "order-enrichment",
    "flinkJobId": "a8f3c2d1e4b5...",
    "yarnApplicationId": "application_1234567890_0001",
    "status": "RUNNING",
    "parallelism": 2,
    "checkpointInterval": 60000,
    "configFilePath": "/app/configs/order-enrichment.json",
    "submittedAt": "2026-03-23T10:15:30Z",
    "updatedAt": "2026-03-23T10:16:05Z",
    "errorMessage": null
  }
]
```

---

### Job Status Polling

The application polls the Flink REST API periodically to refresh job status without requiring the user to refresh the page.

**`JobStatusPoller` — `@Scheduled` component:**

```
Every 30 seconds:
  For each audit record with status SUBMITTING or RUNNING:
    → Call Flink REST API: GET http://<flink.host>:<flink.port>/jobs/<flinkJobId>
    → Map response status to internal enum
    → Update audit record status and updatedAt timestamp
    → If status = FAILED, capture error message from Flink response
```

**Flink REST API status mapping:**

| Flink API Status | Internal Status |
|---|---|
| `INITIALIZING` | `SUBMITTING` |
| `CREATED` | `SUBMITTING` |
| `RUNNING` | `RUNNING` |
| `FINISHED` | `FINISHED` |
| `FAILING` | `RUNNING` (transitional) |
| `FAILED` | `FAILED` |
| `CANCELLING` | `RUNNING` (transitional) |
| `CANCELED` | `CANCELLED` |
| `SUSPENDED` | `FAILED` |
| `RECONCILING` | `RUNNING` |

**Polling configuration:**

```yaml
streaming:
  audit:
    poll-interval-ms: 30000     # How often to refresh job status
    poll-enabled: true          # Disable in test environments
```

---

### Stop / Cancel Behaviour

When the user clicks **Stop** on the dashboard:

```
POST /api/jobs/{id}/stop
       │
       ▼
 Load audit record → verify status is RUNNING
       │
       ▼
 Call Flink REST API:
   PATCH http://<flink.host>:<flink.port>/jobs/<flinkJobId>?mode=cancel
       │
       ▼
 Update audit record status → CANCELLED
       │
       ▼
 Return 200 OK to UI
```

If the job is not in `RUNNING` state, return `409 Conflict` with a message explaining the current status.

---

### Submission Flow Update

The existing `submitJob` flow in `JobController` must be updated to integrate with the audit table:

```
POST /api/jobs/submit
       │
       ▼
 Create audit record with status = SUBMITTING
       │
       ▼
 mapToConfig(request)
       │
       ▼
 orchestrator.submitJob(config)  →  returns JobClient
       │
       ├── On success:
       │     Update audit record:
       │       flinkJobId = jobClient.getJobID().toString()
       │       status     = RUNNING
       │       configFilePath = configs/<jobName>.json
       │       configSnapshot = JSON of SavedJobConfig
       │
       └── On failure:
             Update audit record:
               status = FAILED
               errorMessage = exception message
```

---

### UI Dashboard Design

The dashboard is a new panel in `index.html` (or a separate page) showing a live table of all jobs.

**Columns displayed:**

| Column | Source |
|---|---|
| Job Name | `jobName` |
| Flink Job ID | `flinkJobId` (truncated, copyable) |
| YARN App ID | `yarnApplicationId` |
| Status | `status` (color-coded badge) |
| Parallelism | `parallelism` |
| Submitted At | `submittedAt` (formatted local time) |
| Last Updated | `updatedAt` |
| Actions | Stop / Restart / View Config buttons |

**Status badge colors:**

| Status | Color |
|---|---|
| `SUBMITTING` | Yellow |
| `RUNNING` | Green |
| `FINISHED` | Blue |
| `FAILED` | Red |
| `CANCELLED` | Grey |

**Dashboard auto-refresh:** poll `GET /api/jobs` every 30 seconds to keep the table live without a full page reload.

**View Config action:** opens a modal showing the `configSnapshot` JSON for that job — allows the user to inspect exactly what was deployed.

**Restart action:** resubmits the job using the saved `configSnapshot` as the request body — creates a new audit record while leaving the old one intact as history.

---

### Implementation Checklist

- [ ] Add `spring-boot-starter-data-jpa` and `h2` to `pom.xml`
- [ ] Create `JobAuditRecord` entity with all audit columns
- [ ] Create `JobAuditRepository` (Spring Data JPA)
- [ ] Create `JobAuditService` — `createRecord()`, `updateStatus()`, `findAll()`, `findById()`
- [ ] Create `JobStatusPoller` — `@Scheduled` polling with configurable interval
- [ ] Update `JobController.submitJob()` — capture `JobClient`, write audit record
- [ ] Create `JobDashboardController` — all dashboard REST endpoints
- [ ] Update `index.html` — add dashboard tab/panel with job table and action buttons
- [ ] Update `app.js` — auto-refresh polling, stop/restart handlers, config modal
- [ ] Write unit tests for `JobAuditService`
- [ ] Write integration test: submit job → verify audit record created → stop job → verify status updated

---

## 17. Definition of Done

A feature is considered complete when:

- [ ] All tests pass (unit + integration)
- [ ] No known issues introduced by the change
- [ ] Code reviewed and approved
- [ ] Documentation updated if behavior changed
- [ ] No secrets or passwords logged or persisted in plaintext
- [ ] Flink operator UIDs assigned for all new operators
- [ ] Job config is fully driven by `StreamingJobConfig` — no hardcoded values
