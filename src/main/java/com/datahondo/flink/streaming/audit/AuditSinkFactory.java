package com.datahondo.flink.streaming.audit;

import com.datahondo.flink.streaming.config.AuditConfig;
import com.datahondo.flink.streaming.config.ReconciliationConfig;

/**
 * Creates the appropriate {@link AuditSink} and {@link ReconciliationSink}
 * instances from the supplied configuration.
 *
 * <p>The factory is the single point where sink-type → implementation mapping
 * is resolved, keeping the orchestrator and service layer independent of
 * concrete sink classes.
 */
public interface AuditSinkFactory {

    /**
     * Returns the {@link AuditSink} matching {@code config.getSinkType()}.
     *
     * @param config audit configuration; must not be {@code null}
     * @return a ready-to-use {@link AuditSink}
     * @throws IllegalArgumentException when the requested sink type is unsupported
     */
    AuditSink createAuditSink(AuditConfig config);

    /**
     * Returns the {@link ReconciliationSink} matching
     * {@code config.getSinkType()}.
     *
     * @param config reconciliation configuration; must not be {@code null}
     * @return a ready-to-use {@link ReconciliationSink}
     * @throws IllegalArgumentException when the requested sink type is unsupported
     */
    ReconciliationSink createReconciliationSink(ReconciliationConfig config);
}
