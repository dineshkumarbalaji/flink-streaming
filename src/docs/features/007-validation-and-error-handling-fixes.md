# Feature 007 — Validation & Error Handling Hardening

**Feature ID:** 007  
**Status:** Done  
**Type:** Bug Fix / Improvement  
**Date:** 2026-06-02

---

## Overview

Six issues identified during a code-review audit were resolved across the validation layer,
audit cache, reconciliation service, and error-reporting path. All changes are
backward-compatible; no API contracts or configuration schemas changed.

---

## High Priority Fixes

### Fix 1 — Flink 1.18 REST Bug: Full Exception Chain Now Logged

**Problem:**  
When a Flink job fails during initialization the REST client throws
`UnrecognizedPropertyException`. The existing workaround fell back to the status API, but
if that also failed the original exception was never logged with its stack trace — only
the message string was captured. Root causes were invisible in application logs.

**Fix (`StreamingJobOrchestrator.scheduleCompletionMonitor`):**
- `log.warn(...)` now passes the exception object so the full stack trace is written to application logs.
- When the status-API fallback itself fails, both exceptions are logged at WARN with full stack traces.
- For non-bug exceptions a new `rootCauseMessage()` helper walks the entire cause chain and includes the root cause in the audit `error` metadata field.

**Impact:** Operators can diagnose job initialization failures from application logs without
needing direct access to JobManager logs.

---

### Fix 2 — Savepoint Path Not Validated Before Job Submission

**Problem:**  
User-provided `savepointPath` was passed directly to the Flink configuration without format
or existence checks. An invalid path caused a cryptic Flink runtime failure after submission
had already been accepted with 200 OK.

**Fix (`JobController`):**
- New `validateSavepointPath(String path, List<String> logs)` — mirrors the existing `validateCheckpointDir()` pattern.
- Validates URI format, detects single/double-slash errors, blocks directory traversal (`..`), and for `file:///` URIs verifies that the path exists and is a directory on the local filesystem.
- Called in both `POST /api/jobs/validate` (returns validation log entry) and `POST /api/jobs/submit` (returns `400 Bad Request`).

| URI type | Checks applied |
|----------|----------------|
| `file:///path` | Format, no `..`, path exists, is a directory |
| `hdfs://`, `s3://`, etc. | URI format only (remote existence cannot be verified locally) |
| `file:/p` or `file://p` | Rejected — must use triple slash |

---

### Fix 3 — Kafka Topic Existence Not Checked in the Submit Endpoint

**Problem:**  
`POST /api/jobs/validate` checked Kafka connectivity and topic existence, but
`POST /api/jobs/submit` skipped this step entirely. Jobs could be submitted with
non-existent topics and only fail inside the Flink runtime — after the API had already
returned success.

**Fix (`JobController.submitJob`):**
- After `mapToConfig()`, calls `validatorService.validateConnection(source.getKafka())` for every source and `validatorService.validateConnection(config.getTarget().getKafka())` for the target.
- Returns `400 Bad Request` with a descriptive message on any failure, for example:
  `"Source topic 'orders-raw' validation failed: Topic not found."`

---

## Medium Priority Fixes

### Fix 4 — SQL Validation Only Covered the First Source Table

**Problem:**  
`SqlValidatorService.validateSql()` registered only the first source table in the Flink
validation environment. Multi-source JOIN queries passed SQL validation but failed at Flink
job startup when the secondary table views were not found.

**Fix (`SqlValidatorService` + `JobController`):**
- New `SourceEntry` static inner class encapsulates `tableName`, `schema`,
  `enableWatermark`, and `watermarkMode` for one source.
- `validateSql(String sqlQuery, List<SourceEntry> sources)` registers **all** source tables
  in a single `StreamTableEnvironment`, so JOIN queries referencing multiple tables are
  fully validated.
- `JobController.validateJob()` now builds a `SourceEntry` for every configured source and
  passes the complete list to the validator.

---

### Fix 5 — InMemoryAuditCache Silently Evicted Events

**Problem:**  
When the bounded cache reached its per-job event limit or total-job limit, events and jobs
were silently dropped. Operators had no indication that audit history had been truncated.

**Fix (`InMemoryAuditCache`):**
- Event eviction now emits: `WARN [AUDIT-CACHE] Event evicted for job '<name>' (total evicted: N). Enable a persistent audit sink (JDBC/KAFKA) to retain full history.`
- Job eviction now emits: `WARN [AUDIT-CACHE] Job '<name>' evicted from cache (total jobs evicted: N). Enable a persistent audit sink...`
- New getters `getEvictedEventCount()` and `getEvictedJobCount()` expose running totals for
  future dashboard integration.

---

### Fix 6 — Reconciliation Window Label Reflected Checkpoint Interval, Not Actual Elapsed Time

**Problem:**  
`ReconciliationReport.windowLabel` was set to the checkpoint-interval-derived string
(e.g., `"1h"`) regardless of how long the job actually ran. A job that ran for 45 seconds
showed a window label of `"1h"`, which was misleading in both dashboards and sink output.

**Fix (`ReconciliationService`):**
- `windowLabel` is now computed from the actual elapsed time between `windowStart` and
  `Instant.now()` using the new `formatElapsed(long ms)` helper.
- Format examples: `"234ms"` · `"45s"` · `"2m 34s"` · `"1h 15m"`.
- `windowStart` and `windowEnd` fields in the report are unchanged — only the
  human-readable label is corrected.

---

## Acceptance Criteria

- [x] Job initialization failures show full stack trace in application logs (Fix 1)
- [x] `/validate` rejects invalid savepoint paths with descriptive `❌` log entries (Fix 2)
- [x] `/submit` rejects invalid savepoint paths with `400 Bad Request` (Fix 2)
- [x] `/submit` rejects missing or inaccessible Kafka topics with `400 Bad Request` (Fix 3)
- [x] Multi-source JOIN queries are validated correctly — all source tables registered (Fix 4)
- [x] `WARN` log emitted when audit cache evicts events or jobs (Fix 5)
- [x] `ReconciliationReport.windowLabel` shows actual elapsed time, not checkpoint interval (Fix 6)

---

## Tests Updated

| Class | Change |
|-------|--------|
| `SqlValidatorServiceTest` | Updated `validateSql_throws_whenSqlIsEmpty` and `validateSql_throws_whenSqlIsNull` to use new `List<SourceEntry>` signature |

All other existing tests pass without modification.

---

## Files Changed

| File | Change |
|------|--------|
| `job/StreamingJobOrchestrator.java` | Full exception logging + `rootCauseMessage()` helper |
| `web/JobController.java` | `validateSavepointPath()`, Kafka validation in `/submit` endpoint, all-source SQL validation |
| `web/service/SqlValidatorService.java` | `SourceEntry` inner class, multi-source `validateSql()` |
| `audit/InMemoryAuditCache.java` | Eviction warnings, `AtomicLong` counters, counter getters |
| `audit/ReconciliationService.java` | Actual-elapsed `windowLabel`, `formatElapsed()` helper |
| `test/.../SqlValidatorServiceTest.java` | Signature update |
