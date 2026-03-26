package com.datahondo.flink.streaming.audit;

/**
 * Canonical set of events that flow through the audit pipeline.
 * The ordinal order reflects the natural execution sequence so that
 * audit logs remain naturally sortable by event type name.
 */
public enum AuditEventType {

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /** Job configuration accepted, runId assigned, Flink graph construction started. */
    JOB_SUBMITTED,

    /** Flink job graph submitted to the cluster; execution is in progress. */
    JOB_RUNNING,

    /** Flink job completed gracefully. */
    JOB_COMPLETED,

    /** Flink job terminated with an error. */
    JOB_FAILED,

    /** Job was explicitly cancelled via the API. */
    JOB_CANCELLED,

    // ── Source stage ─────────────────────────────────────────────────────────

    /** Periodic checkpoint of records read from one Kafka source partition/topic. */
    SOURCE_READ,

    /** Records dropped at the source due to schema or deserialization errors. */
    SOURCE_REJECTED,

    // ── Transformation stage ─────────────────────────────────────────────────

    /** Records emitted by the SQL transformation. */
    TRANSFORM_OUTPUT,

    // ── Target stage ─────────────────────────────────────────────────────────

    /** Records successfully serialised and written to the target sink. */
    TARGET_WRITTEN,

    /** Records that failed serialisation or sink write (not yet retried). */
    TARGET_FAILED,

    // ── Reconciliation ───────────────────────────────────────────────────────

    /** A reconciliation window report has been computed and emitted. */
    RECONCILIATION_COMPLETE
}
