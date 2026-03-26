# Feature: Generic Audit & Reconciliation Sink Configuration

## Business Objective
Audit events and reconciliation reports must be routable to different backend systems (log file, Kafka topic, or JDBC table) without code changes. This allows operations teams to wire audit data into existing observability pipelines (e.g., a Kafka topic consumed by a SIEM, or a PostgreSQL table queried by a reporting tool) purely through configuration.

## Input
- **Configuration source**: `application.yml` under `streaming.job.audit` and `streaming.job.reconciliation`
- **Fields** (both audit and reconciliation share the same structure):

| Field | Type | Description |
|---|---|---|
| `enabled` | boolean | Master switch — disables all sink activity when false |
| `sink-type` | String | `LOG` (default), `KAFKA`, or `JDBC` |
| `connection` | String | KAFKA: bootstrap servers (e.g. `kafka:29092`); JDBC: JDBC URL (e.g. `jdbc:postgresql://db:5432/mydb`) |
| `destination` | String | KAFKA: topic name; JDBC: table name |
| `username` | String | JDBC only — database username |
| `password` | String | JDBC only — database password |

- **Reconciliation-only additional fields**:

| Field | Type | Description |
|---|---|---|
| `tolerance-percent` | double | Acceptable % difference before report is flagged (default 0.0 = exact match) |
| `window` | String | Auto-derived from Flink checkpoint interval — not set in YAML |

## Processing Logic
- On job submission, `JobController.mapToConfig()` copies `systemConfig.getAudit()` and `systemConfig.getReconciliation()` into the per-job `StreamingJobConfig`.
- The reconciliation `window` is derived automatically: `ReconciliationConfig.windowFromCheckpointInterval(checkpointIntervalMs)` converts the Flink checkpoint interval (ms) to a human-readable label (`5m`, `1h`, `1d`, etc.).
- `DefaultAuditSinkFactory` reads `AuditConfig.sinkType` / `ReconciliationConfig.sinkType` at job start and instantiates the correct sink:
  - `LOG` → `LogAuditSink` / `LogReconciliationSink` (SLF4J, zero external dependencies)
  - `KAFKA` → `KafkaAuditSink` / `KafkaReconciliationSink` (publishes JSON to `destination` topic via `connection` bootstrap servers)
  - `JDBC` → `JdbcAuditSink` / `JdbcReconciliationSink` (inserts rows into `destination` table via `connection` JDBC URL)
- If `connection` is missing for KAFKA or JDBC, the factory logs a warning and falls back to LOG.

### Reconciliation window derivation
```
checkpointIntervalMs % 86_400_000 == 0  →  Nd  (days)
checkpointIntervalMs % 3_600_000  == 0  →  Nh  (hours)
checkpointIntervalMs % 60_000     == 0  →  Nm  (minutes)
otherwise                               →  Ns  (seconds)
```

## Output
- **LOG sink**: Structured SLF4J log lines at INFO level; visible in container stdout / log aggregation.
- **KAFKA sink**: JSON-serialized `AuditEvent` / `ReconciliationReport` published to the configured topic.
- **JDBC sink**: Row inserted into the configured table on each event / report.

### Expected JDBC schemas
**Audit table** (`destination` value):
```sql
CREATE TABLE audit_events (
    run_id       VARCHAR(120) NOT NULL,
    job_name     VARCHAR(255),
    event_type   VARCHAR(50),
    stage        VARCHAR(50),
    count        BIGINT,
    ts           TIMESTAMP,
    metadata     TEXT
);
```

**Reconciliation table** (`destination` value):
```sql
CREATE TABLE reconciliation_reports (
    run_id          VARCHAR(120) NOT NULL,
    job_name        VARCHAR(255),
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    source_read     BIGINT,
    source_rejected BIGINT,
    transform_out   BIGINT,
    target_written  BIGINT,
    reconciled      BOOLEAN,
    window_label    VARCHAR(20),
    tolerance_pct   DOUBLE PRECISION
);
```

## Edge Cases

| Scenario | Handling Strategy |
|---|---|
| `enabled: false` | `AuditService.initRun()` is a no-op; no sink created |
| `sink-type: KAFKA` but `connection` is empty | Factory warns and falls back to LOG |
| `sink-type: JDBC` but `connection` is empty | Factory warns and falls back to LOG |
| Unknown `sink-type` | Factory warns and falls back to LOG |
| Sink throws on emit | `AuditService` catches and logs; does not propagate to job |
| Checkpoint interval ≤ 0 | `windowFromCheckpointInterval` returns `"n/a"` |

## Test Coverage
- Unit test files:
  - `src/test/java/.../audit/sink/DefaultAuditSinkFactoryTest.java`
  - `src/test/java/.../config/ReconciliationConfigTest.java`
  - `src/test/java/.../audit/AuditServiceTest.java`
- Key scenarios:
  - `createAuditSink_returnsLog_whenTypeIsLog`
  - `createAuditSink_returnsKafka_whenKafkaFullyConfigured`
  - `createAuditSink_returnsLog_whenKafkaRequestedButNoBootstrap`
  - `createAuditSink_returnsJdbc_whenJdbcFullyConfigured`
  - `windowFromCheckpointInterval_formatsCorrectly` (parameterised)
  - `windowFromCheckpointInterval_returnsNa_whenZeroOrNegative`
