package com.datahondo.flink.streaming.audit;

/**
 * Central registry of Flink accumulator names used for audit and reconciliation.
 *
 * <p>Accumulator names follow the pattern {@code audit.<stage>.<tableName>.<metric>}
 * so that multi-source jobs produce distinct, addressable counters per source.
 *
 * <p>The orchestrator reads these accumulators via {@code JobClient.getAccumulators()}
 * after the job has run (or periodically for long-running streaming jobs).
 */
public final class AuditAccumulators {

    private AuditAccumulators() {}

    // ── Name patterns ─────────────────────────────────────────────────────────

    private static final String PREFIX = "audit.";

    /**
     * Records successfully read from a Kafka source and passed downstream.
     *
     * @param tableName the source table name from {@code SourceConfig}
     */
    public static String sourceRead(String tableName) {
        return PREFIX + "source." + sanitize(tableName) + ".read";
    }

    /**
     * Records dropped by the schema validator for a given source table.
     *
     * @param tableName the source table name from {@code SourceConfig}
     */
    public static String sourceRejected(String tableName) {
        return PREFIX + "source." + sanitize(tableName) + ".rejected";
    }

    /**
     * Records emitted by the SQL transformation (measured in the target layer
     * before serialisation, as the transformation result is a lazy Table).
     */
    public static final String TRANSFORM_OUT = PREFIX + "transform.out";

    /**
     * Records successfully serialised and handed to the Kafka producer.
     *
     * @param topic the target Kafka topic
     */
    public static String targetWritten(String topic) {
        return PREFIX + "target." + sanitize(topic) + ".written";
    }

    // ── Aggregated convenience names (across all sources/targets) ─────────────

    /** Aggregated read count across all source tables (used when only one source). */
    public static final String SOURCE_READ_ALL   = PREFIX + "source.all.read";

    /** Aggregated rejected count across all source tables. */
    public static final String SOURCE_REJECTED_ALL = PREFIX + "source.all.rejected";

    /** Aggregated target written count. */
    public static final String TARGET_WRITTEN_ALL  = PREFIX + "target.all.written";

    // ── Internal helpers ──────────────────────────────────────────────────────

    private static String sanitize(String name) {
        return (name == null) ? "unknown" : name.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}
