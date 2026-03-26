package com.datahondo.flink.streaming.audit;

import com.datahondo.flink.streaming.config.AuditConfig;
import com.datahondo.flink.streaming.config.ReconciliationConfig;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

/**
 * Per-execution context created at job submission time.
 *
 * <p>A {@code RunContext} is created once per {@code submitJob()} call and threaded
 * through the orchestration flow to correlate all audit events and accumulator reads
 * that belong to the same execution.
 *
 * <p>The {@code runId} format is: {@code <jobName>-<epochMs>-<uuidShort>}, keeping
 * it sortable by submission time while remaining collision-resistant.
 */
@Data
@Builder
public class RunContext {

    private final String runId;
    private final String jobName;
    private final Instant startTime;
    private final AuditConfig auditConfig;
    private final ReconciliationConfig reconciliationConfig;

    // ── Factory ───────────────────────────────────────────────────────────────

    /**
     * Creates a new {@code RunContext} with a freshly generated {@code runId}.
     *
     * @param jobName              logical job name
     * @param auditConfig          may be {@code null} (audit disabled)
     * @param reconciliationConfig may be {@code null} (reconciliation disabled)
     */
    public static RunContext create(String jobName,
                                   AuditConfig auditConfig,
                                   ReconciliationConfig reconciliationConfig) {
        Instant now = Instant.now();
        String shortUuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String runId = (jobName != null ? jobName.replaceAll("[^a-zA-Z0-9_-]", "_") : "job")
                + "-" + now.toEpochMilli()
                + "-" + shortUuid;
        return RunContext.builder()
                .runId(runId)
                .jobName(jobName)
                .startTime(now)
                .auditConfig(auditConfig)
                .reconciliationConfig(reconciliationConfig)
                .build();
    }

    /** {@code true} when audit is configured and enabled. */
    public boolean isAuditEnabled() {
        return auditConfig != null && auditConfig.isEnabled();
    }

    /** {@code true} when reconciliation is configured and enabled. */
    public boolean isReconciliationEnabled() {
        return reconciliationConfig != null && reconciliationConfig.isEnabled();
    }
}
