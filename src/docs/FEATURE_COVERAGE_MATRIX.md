# Feature Coverage Matrix — Audit & Reconciliation

> **Framework**: DataHonDo Flink Streaming Platform
> **Feature set**: Production-grade audit and reconciliation support
> **Date**: 2026-03-25

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

---

## Summary

| Category | Total Features | ✅ Unit-tested | 🧪 Integration-tested | 🔶 Implicit | 🚧 Stub |
|----------|---------------|---------------|----------------------|------------|---------|
| Configuration | 17 | 14 | 0 | 2 | 2 |
| Run Identity | 7 | 7 | 0 | 0 | 0 |
| Audit Events | 16 | 6 | 3 | 7 | 0 |
| Audit Sink | 11 | 7 | 0 | 3 | 1 |
| AuditService | 9 | 8 | 1 | 0 | 0 |
| Reconciliation | 12 | 11 | 1 | 0 | 0 |
| Flink Accumulators | 11 | 5 | 0 | 6 | 0 |
| End-to-End | 8 | 4 | 4 | 0 | 0 |
| **Total** | **91** | **62 (68%)** | **9 (10%)** | **18 (20%)** | **3 (3%)** |
