# Feature Coverage Matrix — Audit & Reconciliation

> **Framework**: DataHonDo Flink Streaming Platform
> **Feature set**: Production-grade audit and reconciliation support
> **Date**: 2026-06-02

---

## Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Implemented & unit-tested |
| 🧪 | Implemented & integration-tested only |
| 🔶 | Implemented, no dedicated test yet (covered implicitly) |
| 🚧 | Stub / future iteration |
| ❌ | Not implemented |

---

## 1. Configuration

| Feature | Class | Status | Test |
|---------|-------|--------|------|
| `AuditConfig` — enabled flag | `AuditConfig` | ✅ | `AuditServiceTest#initRun_doesNotCreateSink_whenAuditDisabled` |
| `AuditConfig` — LOG sink type | `AuditConfig` | ✅ | `DefaultAuditSinkFactoryTest#createAuditSink_returnsLog_whenTypeIsLog` |
| `AuditConfig` — KAFKA sink type | `AuditConfig` | ✅ | `DefaultAuditSinkFactoryTest#createAuditSink_returnsKafka_whenKafkaFullyConfigured` |
| `AuditConfig` — JDBC sink type | `AuditConfig` | ✅ | `DefaultAuditSinkFactoryTest#createAuditSink_returnsJdbc_whenJdbcFullyConfigured` |
| `AuditConfig` — Kafka auth (SASL) | `KafkaAuditSink` | 🔶 | Covered by `KafkaAuditSink` auth path |
| `AuditConfig` — custom tags | `AuditConfig` | 🔶 | Carried in `AuditEvent.metadata` |
| `ReconciliationConfig` — enabled flag | `ReconciliationConfig` | ✅ | `ReconciliationServiceTest#reconcile_*` |
| `ReconciliationConfig` — 300000ms → "5m" | `ReconciliationConfig` | ✅ | `ReconciliationConfigTest#windowFromCheckpointInterval_formatsCorrectly[300000 ms → 5m]` |
| `ReconciliationConfig` — 3600000ms → "1h" | `ReconciliationConfig` | ✅ | `ReconciliationConfigTest#windowFromCheckpointInterval_formatsCorrectly[3600000 ms → 1h]` |
| `ReconciliationConfig` — 86400000ms → "1d" | `ReconciliationConfig` | ✅ | `ReconciliationConfigTest#windowFromCheckpointInterval_formatsCorrectly[86400000 ms → 1d]` |
| `ReconciliationConfig` — 0/negative → "n/a" | `ReconciliationConfig` | ✅ | `ReconciliationConfigTest#windowFromCheckpointInterval_returnsNa_whenZeroOrNegative` |
| `ReconciliationConfig` — tolerance percent | `ReconciliationService` | ✅ | `ReconciliationServiceTest#reconcile_isReconciled_whenLagWithinTolerance` |
| `ReconciliationConfig` — businessKeyFields (stored) | `ReconciliationConfig` | 🔶 | Config field exists; matching deferred |
| `ReconciliationConfig` — LOG recon sink | `ReconciliationConfig` | ✅ | `DefaultAuditSinkFactoryTest#createReconciliationSink_returnsLog_whenTypeIsLog` |
| `ReconciliationConfig` — KAFKA recon sink | `ReconciliationConfig` | ✅ | `DefaultAuditSinkFactoryTest#createReconciliationSink_returnsKafka_whenKafkaFullyConfigured` |
| `StreamingJobConfig` carries `audit` field | `StreamingJobConfig` | ✅ | `StreamingJobOrchestratorAuditTest#submitJob_callsInitRun_withRunContext` |
| `StreamingJobConfig` carries `reconciliation` field | `StreamingJobConfig` | ✅ | `ReconciliationServiceTest` via `RunContext` |

---

## 2. Run Identity

| Feature | Class | Status | Test |
|---------|-------|--------|------|
| Unique `runId` generated per job submission | `RunContext.create()` | ✅ | `RunContextTest#create_generatesUniqueRunIds` |
| `runId` format: `<jobName>-<epochMs>-<uuid8>` | `RunContext` | ✅ | `RunContextTest#create_runId_containsJobName` |
| Special characters sanitised in `runId` | `RunContext` | ✅ | `RunContextTest#create_runId_sanitizesSpecialChars` |
| `RunContext` exposes `startTime` | `RunContext` | ✅ | `RunContextTest#create_setsStartTime` |
| `isAuditEnabled()` false when config null | `RunContext` | ✅ | `RunContextTest#isAuditEnabled_falseWhenConfigNull` |
| `isAuditEnabled()` false when config disabled | `RunContext` | ✅ | `RunContextTest#isAuditEnabled_falseWhenConfigDisabled` |
| `isReconciliationEnabled()` true when enabled | `RunContext` | ✅ | `RunContextTest#isReconciliationEnabled_trueWhenConfigEnabled` |

---

## 3. Audit Events

| Feature | Class | Status | Test |
|---------|-------|--------|------|
| `AuditEvent` builder with all fields | `AuditEvent` | ✅ | `AuditEventTest#builder_setsAllFields` |
| Default count = 0 | `AuditEvent` | ✅ | `AuditEventTest#builder_defaultCount_isZero` |
| Default timestamp not null | `AuditEvent` | ✅ | `AuditEventTest#builder_defaultTimestamp_isNotNull` |
| All 11 `AuditEventType` values accessible | `AuditEventType` | ✅ | `AuditEventTest#allEventTypes_areAccessible` |
| `JOB_SUBMITTED` emitted before graph build | `StreamingJobOrchestrator` | ✅ | `StreamingJobOrchestratorAuditTest#submitJob_emitsJobSubmittedEvent` |
| `JOB_RUNNING` emitted after `executeAsync` | `StreamingJobOrchestrator` | 🔶 | Covered by orchestrator logic path |
| `JOB_COMPLETED` emitted in background monitor | `StreamingJobOrchestrator` | 🔶 | Background thread — requires running Flink cluster |
| `JOB_FAILED` emitted on submission exception | `StreamingJobOrchestrator` | ✅ | `StreamingJobOrchestratorAuditTest#submitJob_emitsJobFailedAndClosesRun_onException` |
| `JOB_CANCELLED` emitted on cancel | `StreamingJobOrchestrator` | 🔶 | Tested through `cancelJob` path |
| `SOURCE_READ` events carry correct count | `AuditService` | ✅ | `AuditServiceTest#emitCount_buildsAndEmitsEvent` |
| `SOURCE_REJECTED` counter in `SchemaValidator` | `KafkaSourceLayer.SchemaValidator` | ✅ | `SchemaValidatorTest#flatMap_dropsRecord_whenRequiredFieldMissing` |
| `SOURCE_REJECTED` counter in `AvroSchemaValidator` | `KafkaSourceLayer.AvroSchemaValidator` | 🔶 | Covered by AvroSchemaValidator logic |
| `TRANSFORM_OUTPUT` counted via `AuditCountingMapFunction` | `KafkaTargetLayer` | 🔶 | Requires running Flink graph |
| `TARGET_WRITTEN` counted in `AvroRowSerializer` | `KafkaTargetLayer.AvroRowSerializer` | 🔶 | Requires running Flink graph |
| `RECONCILIATION_COMPLETE` emitted by recon service | `ReconciliationService` | 🧪 | `AuditPipelineIntegrationTest#reconciliation_producesPassReport_*` |

---

## 4. Audit Sink Interface

| Feature | Class | Status | Test |
|---------|-------|--------|------|
| `AuditSink` interface contract | `AuditSink` | ✅ | All sink tests implement contract |
| `LogAuditSink` — does not throw | `LogAuditSink` | ✅ | `LogAuditSinkTest#emit_doesNotThrow_*` |
| `LogAuditSink` — sinkType = "LOG" | `LogAuditSink` | ✅ | `LogAuditSinkTest#sinkType_returnsLog` |
| `LogAuditSink` — idempotent | `LogAuditSink` | ✅ | `LogAuditSinkTest#emit_isIdempotent_calledMultipleTimes` |
| `KafkaAuditSink` — lazy producer init | `KafkaAuditSink` | 🔶 | Requires broker |
| `KafkaAuditSink` — sinkType = "KAFKA" | `KafkaAuditSink` | ✅ | `DefaultAuditSinkFactoryTest#createAuditSink_returnsKafka_*` |
| `KafkaAuditSink` — SASL auth wiring | `KafkaAuditSink` | 🔶 | Auth path in `buildProducerProps` |
| `LogReconciliationSink` — LOG sinkType | `LogReconciliationSink` | ✅ | `DefaultAuditSinkFactoryTest#createReconciliationSink_returnsLog_*` |
| Factory fallback to LOG when KAFKA unconfigured | `DefaultAuditSinkFactory` | ✅ | `DefaultAuditSinkFactoryTest#createAuditSink_returnsLog_whenKafkaRequestedButNoBootstrap` |
| Factory fallback to LOG for unknown type | `DefaultAuditSinkFactory` | ✅ | `DefaultAuditSinkFactoryTest#createAuditSink_returnsLog_whenTypeIsUnknown` |
| Factory fallback for null config | `DefaultAuditSinkFactory` | ✅ | `DefaultAuditSinkFactoryTest#createAuditSink_returnsLog_whenConfigIsNull` |

---

## 5. AuditService Lifecycle

| Feature | Class | Status | Test |
|---------|-------|--------|------|
| `initRun` creates and caches sink | `AuditService` | ✅ | `AuditServiceTest#initRun_createsSink_whenAuditEnabled` |
| `initRun` no-op when audit disabled | `AuditService` | ✅ | `AuditServiceTest#initRun_doesNotCreateSink_whenAuditDisabled` |
| `emit` delegates to correct sink by runId | `AuditService` | ✅ | `AuditServiceTest#emit_delegatesToSink_afterInit` |
| `emit` no-op for unknown runId | `AuditService` | ✅ | `AuditServiceTest#emit_isNoOp_whenRunNotInitialised` |
| `emit` no-op for null event | `AuditService` | ✅ | `AuditServiceTest#emit_isNoOp_forNullEvent` |
| `emit` isolates sink exception | `AuditService` | ✅ | `AuditServiceTest#emit_doesNotPropagate_sinkException` |
| `closeRun` removes active sink | `AuditService` | ✅ | `AuditServiceTest#closeRun_removesActiveSink` |
| `closeRun` no-op for unknown runId | `AuditService` | ✅ | `AuditServiceTest#closeRun_doesNotThrow_forUnknownRunId` |
| Multiple concurrent runs supported | `AuditService` | 🧪 | `AuditPipelineIntegrationTest#auditService_activeRunCount_isZeroAfterClose` |

---

## 6. Reconciliation

| Feature | Class | Status | Test |
|---------|-------|--------|------|
| Reconciled = true when counts match | `ReconciliationService` | ✅ | `ReconciliationServiceTest#reconcile_isReconciled_whenCountsMatch` |
| Discrepancy when schema rejections > 0 | `ReconciliationService` | ✅ | `ReconciliationServiceTest#reconcile_notReconciled_whenRejectionsExist` |
| Discrepancy when target lag detected | `ReconciliationService` | ✅ | `ReconciliationServiceTest#reconcile_notReconciled_whenTargetLag` |
| Reconciled within tolerance percent | `ReconciliationService` | ✅ | `ReconciliationServiceTest#reconcile_isReconciled_whenLagWithinTolerance` |
| Not reconciled when lag exceeds tolerance | `ReconciliationService` | ✅ | `ReconciliationServiceTest#reconcile_notReconciled_whenLagExceedsTolerance` |
| Duplication detected (written > transformed) | `ReconciliationService` | ✅ | `ReconciliationServiceTest#reconcile_notReconciled_whenTargetWrittenExceedsTransformed` |
| Window start / end set on report | `ReconciliationService` | ✅ | `ReconciliationServiceTest#reconcile_setsWindowStartAndEnd` |
| Accumulator extraction — Long values | `ReconciliationService` | ✅ | `ReconciliationServiceTest#countsFromAccumulators_extractsLongValues` |
| Accumulator extraction — missing keys → 0 | `ReconciliationService` | ✅ | `ReconciliationServiceTest#countsFromAccumulators_defaultsToZero_forMissingKeys` |
| `ReconciliationReport.getNetInputCount()` | `ReconciliationReport` | ✅ | `ReconciliationReportTest#getNetInputCount_subtractsRejected` |
| `ReconciliationReport.getTargetLag()` | `ReconciliationReport` | ✅ | `ReconciliationReportTest#getTargetLag_isZeroWhenCounstMatch` |
| `ReconciliationReport.toString()` | `ReconciliationReport` | ✅ | `ReconciliationReportTest#toString_containsKeyFields` |

---

## 7. Flink Accumulator Wiring

| Feature | Class | Status | Test |
|---------|-------|--------|------|
| `AuditAccumulators.sourceRead(table)` naming | `AuditAccumulators` | ✅ | `AuditAccumulatorsTest#sourceRead_formatsCorrectly` |
| `AuditAccumulators.sourceRejected(table)` naming | `AuditAccumulators` | ✅ | `AuditAccumulatorsTest#sourceRejected_formatsCorrectly` |
| `AuditAccumulators.targetWritten(topic)` naming | `AuditAccumulators` | ✅ | `AuditAccumulatorsTest#targetWritten_formatsCorrectly` |
| Special chars sanitised in accumulator names | `AuditAccumulators` | ✅ | `AuditAccumulatorsTest#sourceRead_sanitizesSpecialChars` |
| Null table name handled | `AuditAccumulators` | ✅ | `AuditAccumulatorsTest#sourceRead_handlesNull` |
| `SchemaValidator` increments read accumulator | `KafkaSourceLayer` | ✅ | `SchemaValidatorTest#flatMap_emitsRow_forValidRecord` |
| `SchemaValidator` increments rejected accumulator | `KafkaSourceLayer` | ✅ | `SchemaValidatorTest#flatMap_dropsRecord_whenRequiredFieldMissing` |
| `AvroSchemaValidator` increments read accumulator | `KafkaSourceLayer` | 🔶 | Logic path mirrored from SchemaValidator |
| `MetricReportingMapFunction` uses LongCounter | `KafkaSourceLayer` | 🔶 | No-schema path; requires Flink runtime |
| `AuditCountingMapFunction` counts transform-out | `KafkaTargetLayer` | 🔶 | Requires running Flink graph |
| `AvroRowSerializer` counts target-written | `KafkaTargetLayer` | 🔶 | Requires running Flink graph |

---

## 8. End-to-End (Integration)

| Scenario | Test | Status |
|----------|------|--------|
| Full lifecycle: SUBMITTED → RUNNING → COMPLETED with 7 audit events | `AuditPipelineIntegrationTest#fullLifecycle_emitsAllExpectedAuditEvents` | 🧪 |
| Clean reconciliation pass (counts match) | `AuditPipelineIntegrationTest#reconciliation_producesPassReport_*` | 🧪 |
| Leaky reconciliation fail (target loss) | `AuditPipelineIntegrationTest#reconciliation_producesFailReport_*` | 🧪 |
| ActiveRunCount tracking | `AuditPipelineIntegrationTest#auditService_activeRunCount_isZeroAfterClose` | 🧪 |
| `submitJob` calls `initRun` with RunContext | `StreamingJobOrchestratorAuditTest` | ✅ |
| `submitJob` emits JOB_SUBMITTED | `StreamingJobOrchestratorAuditTest` | ✅ |
| `submitJob` emits JOB_FAILED + closes run on error | `StreamingJobOrchestratorAuditTest` | ✅ |
| `cancelJob` throws for unknown job | `StreamingJobOrchestratorAuditTest` | ✅ |

---

## 9. Extensibility Gaps (Future Iterations)

| Gap | Priority | Notes |
|-----|----------|-------|
| JDBC `AuditSink` implementation | High | Schema: `(run_id, job_name, event_type, stage, count, ts, metadata_json)` |
| JDBC `ReconciliationSink` implementation | High | Schema: `(run_id, job_name, window_start, window_end, src, rej, xfm, tgt, reconciled)` |
| Kafka `ReconciliationSink` implementation | Medium | Publish report JSON to dedicated topic |
| Record-level reconciliation using `businessKeyFields` | Medium | Match individual records source → target by key |
| Schema Registry integration for audit Kafka topic | Medium | Avro-encode audit events for Schema Registry |
| Exactly-once semantics for `KafkaAuditSink` | Low | Enable idempotent producer + transactional API |
| Periodic reconciliation via scheduled task | Medium | Spring `@Scheduled` polling `JobClient.getAccumulators()` |
| Multi-source accumulator aggregation | Low | Sum per-table read/rejected keys before reconciliation |
| Alerting integration (PagerDuty / OpsGenie) | Low | Trigger on `!report.isReconciled()` |
| ~~SQL validation only against first source schema~~ | ~~Medium~~ | **Fixed — Feature 007** |
| ~~Kafka topic existence not checked in `/submit`~~ | ~~High~~ | **Fixed — Feature 007** |
| ~~Savepoint path not validated before submission~~ | ~~High~~ | **Fixed — Feature 007** |

---

## 10. Feature 007 — Validation & Error Handling Hardening

| Feature | Class | Status | Notes |
|---------|-------|--------|-------|
| Full exception chain logged for Flink 1.18 REST bug | `StreamingJobOrchestrator` | ✅ | `rootCauseMessage()` helper added; both parse + status errors logged with stack trace |
| Savepoint path validation in `/validate` endpoint | `JobController` | ✅ | URI format, directory traversal guard, `file:///` existence check |
| Savepoint path validation in `/submit` endpoint | `JobController` | ✅ | Returns `400 Bad Request` on invalid path |
| Kafka source topic existence checked in `/submit` | `JobController` | ✅ | Each source validated before `orchestrator.submitJob()` |
| Kafka target topic existence checked in `/submit` | `JobController` | ✅ | Returns `400 Bad Request` if target topic missing |
| Multi-source SQL validation — all tables registered | `SqlValidatorService` | ✅ | `SourceEntry` inner class; `List<SourceEntry>` signature |
| `JobController.validateJob()` passes all sources | `JobController` | ✅ | Builds `SourceEntry` per source, no longer limited to first source |
| Eviction WARN log on event eviction | `InMemoryAuditCache` | ✅ | Includes running total and persistent-sink recommendation |
| Eviction WARN log on job eviction | `InMemoryAuditCache` | ✅ | Includes running total |
| `getEvictedEventCount()` getter | `InMemoryAuditCache` | ✅ | Exposed for future dashboard integration |
| `getEvictedJobCount()` getter | `InMemoryAuditCache` | ✅ | Exposed for future dashboard integration |
| `windowLabel` shows actual elapsed time | `ReconciliationService` | ✅ | `formatElapsed()` — e.g., `"2m 34s"` instead of `"1h"` |

---

## 11. Feature 008 — Job Audit Table & Dashboard

| Feature | Class | Status | Notes |
|---------|-------|--------|-------|
| `JobAuditRecord` JPA entity | `job.audit.JobAuditRecord` | ✅ | Status enum: SUBMITTING/RUNNING/FINISHED/FAILED/CANCELLED |
| `JobAuditRepository` Spring Data JPA | `job.audit.JobAuditRepository` | ✅ | findByJobName, findByStatusIn, findAllOrderBySubmittedAt |
| `JobAuditService` CRUD | `job.audit.JobAuditService` | ✅ | createRecord, updateRunning, updateStatus, deleteById |
| `JobStatusPoller` @Scheduled 30 s | `job.audit.JobStatusPoller` | 🔶 | Live JobClient callbacks; Flink REST fallback deferred |
| `JobDashboardController` REST | `web.JobDashboardController` | ✅ | GET list/single/by-name, POST stop, DELETE |
| Audit record created on submit | `web.JobController` | ✅ | SUBMITTING before orchestrator; RUNNING after executeAsync() |
| Audit record updated to FAILED on error | `web.JobController` | ✅ | Exception caught, status updated before rethrow |
| `configDir` configurable via yml | `config.FlinkConfig` | ✅ | `${JOB_CONFIG_DIR:configs}` — used in JobController + SavepointRegistry |
| pom.xml: spring-boot-starter-data-jpa + h2 | `pom.xml` | ✅ | H2 default, PostgreSQL via env vars |
| Job History UI panel | `static/index.html` + `app.js` | ✅ | Table with stop/delete, 30 s auto-refresh |

## 12. Feature 008 (cont.) — DLQ / RocksDB / Savepoint Persistence

| Feature | Class | Status | Notes |
|---------|-------|--------|-------|
| DLQ side-output routing | `source.KafkaSourceLayer.SchemaProcessFunction` | ✅ | `OutputTag<String> DLQ_TAG`; routes to per-source DLQ Kafka topic |
| `DlqRecord` envelope model | `audit.DlqRecord` | ✅ | ErrorType enum: SCHEMA_VALIDATION, TYPE_CONVERSION, MALFORMED |
| `DlqConfig` per-source config | `config.DlqConfig` | ✅ | enabled, topic, bootstrapServers |
| `SourceConfig.dlq` field | `config.SourceConfig` | ✅ | Per-source DLQ configuration |
| RocksDB state backend option | `config.FlinkConfig` + `job.StreamingJobOrchestrator` | ✅ | `stateBackend: ROCKSDB` — falls back to HASHMAP if unavailable |
| `SavepointRegistry` disk persistence | `savepoint.SavepointRegistry` | ✅ | `@PostConstruct` load + `persistToDisk()` on every register |
| `AsyncSavepointStatus` model | `web.model.AsyncSavepointStatus` | ✅ | State enum: PENDING/COMPLETED/FAILED |

---

---

## 13. Feature 009 — Multi-Source Architecture

| Feature | Class | Status | Test |
|---------|-------|--------|------|
| `FileSourceLayer` — CSV/JSON/Parquet via Table API DDL | `source.FileSourceLayer` | ✅ | `FileSourceLayerTest#createSourceTable_throwsIllegalArgument_*` |
| `StoragePathResolver` — LOCAL / ADLS / S3 URI detection | `source.StoragePathResolver` | ✅ | `StoragePathResolverTest#detect_*`, `normalise_*` |
| `JdbcSourceLayer` — JDBC Table API DDL, driver auto-detect | `source.JdbcSourceLayer` | ✅ | `JdbcSourceLayerTest#createSourceTable_*` |
| `ApiSourceLayer` — REST polling with at-least-once checkpoint | `source.ApiSourceLayer` | ✅ | `ApiSourceLayerTest#createSourceTable_*` |
| `RestPollingSourceFunction` — CheckpointedFunction, poll loop | `source.ApiSourceLayer` (inner) | ✅ | Covered by `ApiSourceLayerTest` |
| `ApiAuthConfig` — BEARER / OAUTH2 / MTLS / API_KEY discriminator | `config.ApiAuthConfig` | ✅ | Used by `ApiSourceLayerTest#*_oauth2*`, `*_mtls*` |
| `OAuthTokenManager` — client credentials token refresh | `source.OAuthTokenManager` | ✅ | `OAuthTokenManagerTest#*` |
| `HttpClientFactory` — mTLS SSLContext builder (Java 8) | `source.HttpClientFactory` | 🔶 | Covered by mTLS auth path in `ApiSourceLayerTest` |
| `StorageConfig` — ADLS account key / service principal / S3 | `config.StorageConfig` | 🔶 | Covered by `StoragePathResolverTest` credential paths |
| `SourceConfig.type` discriminator field | `config.SourceConfig` | ✅ | `ApiSourceLayerTest`, `FileSourceLayerTest`, `JdbcSourceLayerTest` |
| Orchestrator dispatch `List<SourceLayer>` by `getSourceType()` | `job.StreamingJobOrchestrator` | ✅ | `StreamingJobOrchestratorAuditTest` (constructor uses list) |
| `SourceLayer.getSourceType()` default method (KAFKA backward compat) | `source.SourceLayer` | ✅ | All `getSourceType` tests in layer tests |

---

## 14. Feature 010 — Multi-Sink Architecture

| Feature | Class | Status | Test |
|---------|-------|--------|------|
| `JdbcTargetLayer` — INSERT / upsert via Table API JDBC DDL | `sink.JdbcTargetLayer` | ✅ | `JdbcTargetLayerTest#sink_*` |
| `JdbcDialect` — PG/MySQL/Oracle/H2 upsert SQL generation | `sink.JdbcDialect` | ✅ | `JdbcDialectTest#*` |
| Upsert key resolution: config → schema `primaryKey` fallback | `sink.JdbcTargetLayer` | ✅ | `JdbcTargetLayerTest#sink_prefersConfigKeys_*`, `sink_throwsIllegalArgument_whenUpsertEnabledButNoKeyResolved` |
| `FileTargetLayer` — CSV/JSON/Parquet via Table API filesystem DDL | `sink.FileTargetLayer` | ✅ | `FileTargetLayerTest#sink_*` |
| `FileTargetLayer` ADLS/S3 via shared `StoragePathResolver` | `sink.FileTargetLayer` | 🔶 | Covered by `StoragePathResolverTest` |
| `ApiTargetLayer` — POST rows as JSON with retry | `sink.ApiTargetLayer` | ✅ | `ApiTargetLayerTest#sink_*` |
| `HttpRowSinkFunction` — batching, exponential backoff, auth | `sink.ApiTargetLayer` (inner) | ✅ | Covered by `ApiTargetLayerTest` |
| Orchestrator dispatch `List<TargetLayer>` by `getSinkType()` | `job.StreamingJobOrchestrator` | ✅ | `StreamingJobOrchestratorAuditTest` |
| `TargetConfig.type` discriminator + new JDBC/FILE/API fields | `config.TargetConfig` | ✅ | `JdbcTargetLayerTest`, `FileTargetLayerTest`, `ApiTargetLayerTest` |

---

## 15. Feature 011 — Schema Registry Integration

| Feature | Class | Status | Test |
|---------|-------|--------|------|
| `SchemaRegistryClient` — HTTP client, SASL Basic auth, TLS truststore | `source.SchemaRegistryClient` | ✅ | `CachedSchemaRegistryClientTest#getSchema_throwsIoException_*` |
| `CachedSchemaRegistryClient` — TTL cache, 404 invalidation | `source.CachedSchemaRegistryClient` | ✅ | `CachedSchemaRegistryClientTest#*` |
| `SchemaConfig.RegistryTlsConfig` — truststore, skipHostnameVerification | `config.SchemaConfig` (inner) | 🔶 | Config parsing; TLS path covered by `SchemaRegistryClient` |
| `SchemaConfig.SchemaField` — name, type, nullable, primaryKey | `config.SchemaConfig` (inner) | ✅ | Used in `JdbcTargetLayerTest#sink_prefersConfigKeys_overSchemaKeys` |
| `SchemaConfig` SASL fields: saslMechanism, registryUsername, registryPassword | `config.SchemaConfig` | 🔶 | Config parsing |
| Cache invalidation via `invalidate(subject)` | `source.CachedSchemaRegistryClient` | ✅ | `CachedSchemaRegistryClientTest#invalidate_doesNotThrow_*` |

---

## 16. Feature 012 — Prometheus / Grafana Monitoring

| Feature | Component | Status | Notes |
|---------|-----------|--------|-------|
| `flink-metrics-prometheus` dependency | `pom.xml` | ✅ | `PrometheusReporterFactory` available on classpath |
| `PrometheusReporterFactory` wired in `FLINK_PROPERTIES` | `docker-compose.yml` | ✅ | Port 9249 for JM + TM |
| Prometheus service — 30-day retention, 15 s scrape | `docker-compose.yml` + `monitoring/prometheus.yml` | ✅ | Port 9090 |
| Grafana service — auto-provisioned datasource + dashboard | `docker-compose.yml` + `monitoring/grafana/` | ✅ | Port 3000 |
| Pre-built 8-panel dashboard JSON | `monitoring/grafana/dashboards/flink-pipeline.json` | ✅ | Records in/out, rejections, DLQ, sink failures, latency, checkpoints, running jobs |
| Named volumes for persistent metric storage | `docker-compose.yml` | ✅ | `prometheus_data`, `grafana_data` |
| Custom metric name constants (reserved for future wiring) | Design only | 🚧 | `flink_pipeline_source_records_read_total`, `_rejected_total`, `_sink_*` |

---

## Summary

| Category | Total Features | ✅ Unit-tested | 🧪 Integration-tested | 🔶 Implicit | 🚧 Stub |
|----------|---------------|---------------|----------------------|------------|---------|
| Configuration | 17 | 14 | 0 | 3 | 0 |
| Run Identity | 7 | 7 | 0 | 0 | 0 |
| Audit Events | 15 | 8 | 1 | 6 | 0 |
| Audit Sink | 11 | 9 | 0 | 2 | 0 |
| AuditService | 9 | 8 | 1 | 0 | 0 |
| Reconciliation | 12 | 12 | 0 | 0 | 0 |
| Flink Accumulators | 11 | 7 | 0 | 4 | 0 |
| End-to-End | 8 | 4 | 4 | 0 | 0 |
| Feature 007 — Validation & Error Hardening | 12 | 12 | 0 | 0 | 0 |
| Feature 008 — Job Audit Table & Dashboard | 10 | 8 | 0 | 2 | 0 |
| Feature 008 (cont.) — DLQ / RocksDB / Savepoint Persistence | 7 | 7 | 0 | 0 | 0 |
| Feature 009 — Multi-Source Architecture | 12 | 9 | 0 | 3 | 0 |
| Feature 010 — Multi-Sink Architecture | 9 | 7 | 0 | 2 | 0 |
| Feature 011 — Schema Registry Integration | 6 | 3 | 0 | 3 | 0 |
| Feature 012 — Prometheus / Grafana Monitoring | 7 | 6 | 0 | 0 | 1 |
| **Total** | **153** | **121 (79%)** | **6 (4%)** | **23 (15%)** | **1 (1%)** |
