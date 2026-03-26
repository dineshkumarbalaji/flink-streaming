package com.datahondo.flink.streaming.audit;

/**
 * Pluggable persistence contract for {@link AuditEvent} records.
 *
 * <p>Implementations must be thread-safe: the orchestrator may call
 * {@link #emit(AuditEvent)} from multiple threads (e.g. the HTTP request thread
 * plus the background reconciliation scheduler).
 *
 * <p>Provided implementations:
 * <ul>
 *   <li>{@code LogAuditSink}  – writes structured JSON to SLF4J (default, no dependencies)</li>
 *   <li>{@code KafkaAuditSink} – publishes to a Kafka topic (requires broker config)</li>
 * </ul>
 *
 * <p>Extension point: implement this interface, annotate with {@code @Component},
 * and register via {@code DefaultAuditSinkFactory}.
 */
public interface AuditSink {

    /**
     * Persists a single audit event.
     *
     * <p>Implementations should never throw checked exceptions; wrap any I/O
     * errors in a {@link RuntimeException} or log-and-swallow depending on the
     * criticality of the audit trail in the deployment.
     *
     * @param event the audit event to persist (never {@code null})
     */
    void emit(AuditEvent event);

    /**
     * Human-readable type token used for logging and factory lookup.
     * Examples: {@code "LOG"}, {@code "KAFKA"}, {@code "JDBC"}.
     */
    String sinkType();
}
