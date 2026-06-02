package com.datahondo.flink.streaming.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe, bounded in-memory cache retaining the most recent audit events
 * and reconciliation reports per job. Used primarily to serve Dashboard UI API requests.
 *
 * <p>Two independent bounds are enforced:
 * <ul>
 *   <li><b>maxEventsPerJob</b> — max number of {@link AuditEvent} entries kept per job name.
 *       Oldest events are evicted when this limit is reached.</li>
 *   <li><b>maxJobs</b> — max number of distinct job names tracked.
 *       When a new job is added beyond this limit the oldest job entry
 *       (by insertion order) is removed together with its reconciliation report.</li>
 * </ul>
 */
@Component
public class InMemoryAuditCache {

    private static final Logger log = LoggerFactory.getLogger(InMemoryAuditCache.class);

    private static final int DEFAULT_MAX_EVENTS_PER_JOB = 50;
    private static final int DEFAULT_MAX_JOBS = 100;

    private final int maxEventsPerJob;
    private final int maxJobs;

    private final AtomicLong evictedEventCount = new AtomicLong();
    private final AtomicLong evictedJobCount   = new AtomicLong();

    /**
     * Insertion-ordered map of job → event deque.
     * All access is guarded by {@code synchronized(this)} so that job-level
     * eviction (which spans both eventCache and reportCache) is atomic.
     */
    private final LinkedHashMap<String, Deque<AuditEvent>> eventCache = new LinkedHashMap<>();
    private final Map<String, ReconciliationReport> reportCache = new ConcurrentHashMap<>();

    public InMemoryAuditCache() {
        this(DEFAULT_MAX_EVENTS_PER_JOB, DEFAULT_MAX_JOBS);
    }

    public InMemoryAuditCache(int maxEventsPerJob) {
        this(maxEventsPerJob, DEFAULT_MAX_JOBS);
    }

    public InMemoryAuditCache(int maxEventsPerJob, int maxJobs) {
        this.maxEventsPerJob = maxEventsPerJob;
        this.maxJobs = maxJobs;
    }

    public synchronized void addEvent(String jobName, AuditEvent event) {
        if (jobName == null || event == null) return;

        if (!eventCache.containsKey(jobName)) {
            evictOldestJobIfNeeded();
        }

        Deque<AuditEvent> deque = eventCache.computeIfAbsent(jobName, k -> new LinkedList<>());
        if (deque.size() >= maxEventsPerJob) {
            deque.pollFirst();
            long total = evictedEventCount.incrementAndGet();
            log.warn("[AUDIT-CACHE] Event evicted for job '{}' (total evicted: {})."
                    + " Enable a persistent audit sink (JDBC/KAFKA) to retain full history.",
                    jobName, total);
        }
        deque.addLast(event);
    }

    public synchronized List<AuditEvent> getEvents(String jobName) {
        if (jobName == null) return new ArrayList<>();
        Deque<AuditEvent> deque = eventCache.get(jobName);
        if (deque == null) return new ArrayList<>();
        return new ArrayList<>(deque);
    }

    public void setLatestReport(String jobName, ReconciliationReport report) {
        if (jobName == null || report == null) return;
        reportCache.put(jobName, report);
    }

    public ReconciliationReport getLatestReport(String jobName) {
        if (jobName == null) return null;
        return reportCache.get(jobName);
    }

    // ── private ───────────────────────────────────────────────────────────────

    /** Must be called while holding {@code this} monitor. */
    private void evictOldestJobIfNeeded() {
        if (eventCache.size() >= maxJobs) {
            String oldest = eventCache.keySet().iterator().next(); // LinkedHashMap insertion order
            eventCache.remove(oldest);
            reportCache.remove(oldest);
            long total = evictedJobCount.incrementAndGet();
            log.warn("[AUDIT-CACHE] Job '{}' evicted from cache (total jobs evicted: {})."
                    + " Enable a persistent audit sink (JDBC/KAFKA) to retain full history.",
                    oldest, total);
        }
    }

    public long getEvictedEventCount() { return evictedEventCount.get(); }
    public long getEvictedJobCount()   { return evictedJobCount.get(); }
}
