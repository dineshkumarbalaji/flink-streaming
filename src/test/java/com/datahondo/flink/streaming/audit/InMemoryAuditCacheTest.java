package com.datahondo.flink.streaming.audit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryAuditCacheTest {

    private InMemoryAuditCache cache;

    @BeforeEach
    void setUp() {
        cache = new InMemoryAuditCache(3);
    }

    @Test
    void addEvent_evictsOldestWhenMaxReached() {
        cache.addEvent("job1", AuditEvent.builder().jobName("job1").runId("run1").eventType(AuditEventType.JOB_SUBMITTED).build());
        cache.addEvent("job1", AuditEvent.builder().jobName("job1").runId("run1").eventType(AuditEventType.JOB_RUNNING).build());
        cache.addEvent("job1", AuditEvent.builder().jobName("job1").runId("run1").eventType(AuditEventType.SOURCE_READ).build());
        cache.addEvent("job1", AuditEvent.builder().jobName("job1").runId("run1").eventType(AuditEventType.JOB_COMPLETED).build());

        List<AuditEvent> events = cache.getEvents("job1");
        assertEquals(3, events.size());
        assertEquals(AuditEventType.JOB_RUNNING, events.get(0).getEventType());
        assertEquals(AuditEventType.JOB_COMPLETED, events.get(2).getEventType());
    }

    @Test
    void addReport_storesMostRecentPerJob() {
        ReconciliationReport r1 = ReconciliationReport.builder().jobName("job1").runId("run1").windowStart(Instant.now()).windowEnd(Instant.now()).reconciled(false).build();
        ReconciliationReport r2 = ReconciliationReport.builder().jobName("job1").runId("run1").windowStart(Instant.now()).windowEnd(Instant.now()).reconciled(true).build();

        cache.setLatestReport("job1", r1);
        ReconciliationReport retrieved1 = cache.getLatestReport("job1");
        assertFalse(retrieved1.isReconciled());

        cache.setLatestReport("job1", r2);
        ReconciliationReport retrieved2 = cache.getLatestReport("job1");
        assertTrue(retrieved2.isReconciled());
    }

    @Test
    void getEvents_returnsEmptyListForUnknownJob() {
        assertTrue(cache.getEvents("non-existent").isEmpty());
    }

    // ── Job-level eviction ────────────────────────────────────────────────────

    /**
     * When more distinct job names than maxJobs are added, the oldest job entry
     * must be evicted to prevent unbounded memory growth.
     */
    @Test
    void addEvent_evictsOldestJobEntry_whenMaxJobsExceeded() {
        InMemoryAuditCache boundedCache = new InMemoryAuditCache(50, 2);

        boundedCache.addEvent("job1", makeEvent("job1"));
        boundedCache.addEvent("job2", makeEvent("job2"));
        boundedCache.addEvent("job3", makeEvent("job3")); // job1 must be evicted

        assertTrue(boundedCache.getEvents("job1").isEmpty(),
                "job1 must be evicted when maxJobs=2 and job3 is added");
        assertFalse(boundedCache.getEvents("job2").isEmpty());
        assertFalse(boundedCache.getEvents("job3").isEmpty());
    }

    @Test
    void addEvent_doesNotEvict_whenJobCountWithinMax() {
        InMemoryAuditCache boundedCache = new InMemoryAuditCache(50, 3);

        boundedCache.addEvent("job1", makeEvent("job1"));
        boundedCache.addEvent("job2", makeEvent("job2"));
        boundedCache.addEvent("job3", makeEvent("job3"));

        assertFalse(boundedCache.getEvents("job1").isEmpty(), "job1 must be retained");
        assertFalse(boundedCache.getEvents("job2").isEmpty());
        assertFalse(boundedCache.getEvents("job3").isEmpty());
    }

    @Test
    void addEvent_evictsOldestJobReportToo_whenMaxJobsExceeded() {
        InMemoryAuditCache boundedCache = new InMemoryAuditCache(50, 2);
        ReconciliationReport report = ReconciliationReport.builder()
                .jobName("job1").runId("run1")
                .windowStart(java.time.Instant.now()).windowEnd(java.time.Instant.now())
                .reconciled(true).build();
        boundedCache.setLatestReport("job1", report);

        boundedCache.addEvent("job1", makeEvent("job1"));
        boundedCache.addEvent("job2", makeEvent("job2"));
        boundedCache.addEvent("job3", makeEvent("job3")); // evicts job1

        assertNull(boundedCache.getLatestReport("job1"),
                "Reconciliation report for evicted job1 must also be removed");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private AuditEvent makeEvent(String jobName) {
        return AuditEvent.builder()
                .jobName(jobName)
                .runId("run-" + jobName)
                .eventType(AuditEventType.JOB_SUBMITTED)
                .build();
    }
}
