package com.datahondo.flink.streaming.audit;

/**
 * Pluggable persistence contract for {@link ReconciliationReport} records.
 *
 * <p>Mirrors the design of {@link AuditSink}: stateless, thread-safe, with a
 * type token for factory dispatch.
 */
public interface ReconciliationSink {

    /**
     * Persists a single reconciliation report.
     *
     * @param report the report to persist (never {@code null})
     */
    void emit(ReconciliationReport report);

    /**
     * Type token matching the {@code ReconciliationConfig#sinkType} value
     * that this sink handles.
     */
    String sinkType();
}
