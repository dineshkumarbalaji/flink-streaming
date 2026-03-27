package com.datahondo.flink.streaming.audit;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Count-based reconciliation report for a single time window of a job execution.
 *
 * <p>The report captures record counts at each pipeline stage and computes
 * whether the numbers are within the configured tolerance.  Discrepancies
 * are described as human-readable strings so they can be shipped to any
 * downstream system (log, Kafka, JDBC, alerting).
 *
 * <p>Reconciliation identity:
 * <ul>
 *   <li>sourceReadCount – records read from Kafka (all sources combined)</li>
 *   <li>schemaRejectedCount – records dropped by schema validators</li>
 *   <li>transformedCount – records emitted by the SQL transformation</li>
 *   <li>targetWrittenCount – records durably written to the target sink</li>
 * </ul>
 */
@Data
@Builder
public class ReconciliationReport {

    private final String runId;
    private final String jobName;

    // ── Window ───────────────────────────────────────────────────────────────

    private final Instant windowStart;
    private final Instant windowEnd;

    /** Human-readable window label, e.g. "1h", "5m". */
    private final String windowLabel;

    // ── Counts ───────────────────────────────────────────────────────────────

    private final long sourceReadCount;
    private final long schemaRejectedCount;
    private final long transformedCount;
    private final long targetWrittenCount;

    // ── Result ───────────────────────────────────────────────────────────────

    /**
     * {@code true} when all counts are within the configured tolerance and
     * no discrepancies were found.
     */
    private final boolean reconciled;

    /**
     * Human-readable list of count mismatches or data-loss indicators.
     * Empty when {@link #isReconciled()} is {@code true}.
     */
    @Builder.Default
    private final List<String> discrepancies = new ArrayList<>();

    // ── Derived helpers ───────────────────────────────────────────────────────

    /** Records that entered the pipeline but were either rejected or lost. */
    public long getNetInputCount() {
        return sourceReadCount - schemaRejectedCount;
    }

    /**
     * Difference between records that left the transformation and records
     * confirmed written to the target.  A positive value indicates potential
     * data loss in the target layer; negative is unexpected and flags duplication.
     */
    public long getTargetLag() {
        return transformedCount - targetWrittenCount;
    }

    @Override
    public String toString() {
        return String.format(
            "ReconciliationReport{runId=%s, job=%s, window=[%s → %s](%s), "
            + "src=%d, rejected=%d, transformed=%d, written=%d, "
            + "reconciled=%b, discrepancies=%s}",
            runId, jobName, windowStart, windowEnd, windowLabel,
            sourceReadCount, schemaRejectedCount, transformedCount, targetWrittenCount,
            reconciled, discrepancies);
    }
}
