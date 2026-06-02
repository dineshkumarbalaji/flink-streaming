# Feature: Bug Fixes — Thread Safety, Avro Field Mapping, Cache Eviction

**Feature ID:** 005  
**Status:** Done  
**Type:** Bug Fix

---

## Problem Statements

### Bug 1 — runningJobs race condition (thread safety)

`StreamingJobOrchestrator.runningJobs` was a plain `LinkedHashMap`. The `@Scheduled` poller
(`pollRunningJobs`) reads and iterates this map on the scheduler thread while `submitJob` and
`cleanup` mutate it on HTTP threads. This is a race that can produce
`ConcurrentModificationException` under load.

**Fix:** Changed to `ConcurrentHashMap`. Insertion order was not a correctness requirement
(the `getRunningJobs` API returns a list with no guaranteed order), so the switch is safe.

### Bug 2 — AvroRowSerializer silent field corruption

`KafkaTargetLayer.AvroRowSerializer.map()` iterated Avro schema fields by index and read
`row.getField(i)` positionally. When the SQL `SELECT` column order differed from the Avro
schema field order, values were written to the wrong fields without any error.

**Fix:** Replaced positional `row.getField(i)` with name-based `row.getField(field.name())`.
Rows produced by `tableEnv.toDataStream()` in Flink 1.18 are named rows, so name-based
lookup is correct and reliable.

### Bug 3 — InMemoryAuditCache unbounded job map growth

`InMemoryAuditCache.eventCache` was bounded per-job (max 50 events) but the number of
distinct job names was never capped. A long-running deployment with many unique job names
would accumulate entries indefinitely.

**Fix:** Added a `maxJobs` bound (default 100). When a new job name is introduced beyond
the cap, the oldest job entry is evicted (insertion-order, via `LinkedHashMap`) together
with its reconciliation report.

---

## Acceptance Criteria

- [ ] Concurrent calls to `pollRunningJobs` and `submitJob` do not throw `ConcurrentModificationException`
- [ ] `AvroRowSerializer` maps Avro field values by name — column order in SQL result does not affect output correctness
- [ ] `InMemoryAuditCache` with `maxJobs=N` evicts the oldest job entry when job `N+1` is added
- [ ] Eviction also removes the corresponding reconciliation report from `reportCache`
- [ ] All existing unit tests continue to pass

---

## Tests Added

| Test class | Method | Covers |
|---|---|---|
| `StreamingJobOrchestratorAuditTest` | `pollRunningJobs_concurrentWithSubmit_doesNotThrowConcurrentModificationException` | Bug 1 |
| `AvroRowSerializerTest` | `map_withFieldsInDifferentOrderThanAvroSchema_mapsValuesByName` | Bug 2 |
| `InMemoryAuditCacheTest` | `addEvent_evictsOldestJobEntry_whenMaxJobsExceeded` | Bug 3 |
| `InMemoryAuditCacheTest` | `addEvent_doesNotEvict_whenJobCountWithinMax` | Bug 3 |
| `InMemoryAuditCacheTest` | `addEvent_evictsOldestJobReportToo_whenMaxJobsExceeded` | Bug 3 |
