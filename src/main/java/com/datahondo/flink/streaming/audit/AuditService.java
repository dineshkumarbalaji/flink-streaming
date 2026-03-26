package com.datahondo.flink.streaming.audit;

import com.datahondo.flink.streaming.config.AuditConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spring service that routes {@link AuditEvent} records to the correct
 * {@link AuditSink} for each active job run.
 *
 * <p>Lifecycle per job execution:
 * <ol>
 *   <li>{@link #initRun(RunContext)} — called by the orchestrator before job submission;
 *       creates and caches the sink for this run.</li>
 *   <li>{@link #emit(AuditEvent)} — called at each audit point; looks up the cached sink
 *       by {@code runId} and delegates.</li>
 *   <li>{@link #closeRun(String)} — called after the final reconciliation report is emitted;
 *       removes the sink from the cache to prevent leaks.</li>
 * </ol>
 *
 * <p>Thread-safety: the sink map is a {@link ConcurrentHashMap}; individual sink
 * implementations are expected to be thread-safe (see {@link AuditSink}).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {

    private final AuditSinkFactory sinkFactory;

    /** Active sinks keyed by runId.  Cleaned up in {@link #closeRun(String)}. */
    private final Map<String, AuditSink> activeSinks = new ConcurrentHashMap<>();

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Initialises the audit sink for a new job run.
     * Safe to call even when {@code runContext.isAuditEnabled()} is {@code false}.
     */
    public void initRun(RunContext runContext) {
        if (!runContext.isAuditEnabled()) {
            log.debug("[AUDIT] Audit disabled for runId={}", runContext.getRunId());
            return;
        }
        AuditConfig cfg = runContext.getAuditConfig();
        AuditSink sink = sinkFactory.createAuditSink(cfg);
        activeSinks.put(runContext.getRunId(), sink);
        log.info("[AUDIT] Initialised {} sink for runId={}", sink.sinkType(), runContext.getRunId());
    }

    /**
     * Emits an {@link AuditEvent} through the sink associated with the event's {@code runId}.
     * If no sink is registered (audit disabled or run already closed) the call is a no-op.
     */
    public void emit(AuditEvent event) {
        if (event == null) return;
        AuditSink sink = activeSinks.get(event.getRunId());
        if (sink == null) {
            log.debug("[AUDIT] No active sink for runId={} — event dropped (type={})",
                    event.getRunId(), event.getEventType());
            return;
        }
        try {
            sink.emit(event);
        } catch (Exception e) {
            log.warn("[AUDIT] Sink threw exception for runId={}: {}", event.getRunId(), e.getMessage());
        }
    }

    /**
     * Releases the sink cached for {@code runId}.
     * Should be called after the final audit event for a run has been emitted.
     */
    public void closeRun(String runId) {
        AuditSink removed = activeSinks.remove(runId);
        if (removed != null) {
            log.info("[AUDIT] Closed {} sink for runId={}", removed.sinkType(), runId);
        }
    }

    // ── Convenience builders ──────────────────────────────────────────────────

    /**
     * Builds and emits a count-based audit event in a single call.
     *
     * @param runId     run identifier
     * @param jobName   logical job name
     * @param eventType audit event type
     * @param stage     pipeline stage label (e.g. {@code "source/orders"})
     * @param count     record count for this event
     * @param metadata  optional extra key-value pairs (may be {@code null})
     */
    public void emitCount(String runId, String jobName, AuditEventType eventType,
                          String stage, long count, Map<String, String> metadata) {
        emit(AuditEvent.builder()
                .runId(runId)
                .jobName(jobName)
                .eventType(eventType)
                .stage(stage)
                .count(count)
                .metadata(metadata)
                .build());
    }

    /** Emits a zero-count lifecycle event. */
    public void emitLifecycle(String runId, String jobName, AuditEventType eventType,
                               Map<String, String> metadata) {
        emitCount(runId, jobName, eventType, "orchestrator", 0L, metadata);
    }

    // ── Visibility for tests ──────────────────────────────────────────────────

    /** Returns the number of currently active (non-closed) runs.  Useful in tests. */
    int activeRunCount() {
        return activeSinks.size();
    }
}
