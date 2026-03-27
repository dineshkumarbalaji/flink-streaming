package com.datahondo.flink.streaming.config;

import lombok.Data;

import java.util.List;

/**
 * Controls count-based reconciliation across the source → transform → target pipeline.
 *
 * Supported sink types:
 *   LOG   – SLF4J structured log line (default, zero external dependencies)
 *   KAFKA – connection = bootstrap-servers, destination = topic
 *   JDBC  – connection = jdbc-url,          destination = table name
 */
@Data
public class ReconciliationConfig {

    /** Master switch. */
    private boolean enabled = true;

    /** Sink backend. One of: LOG, KAFKA, JDBC. Defaults to LOG. */
    private String sinkType = "LOG";

    /**
     * Generic connection string, interpreted by sink-type:
     *   KAFKA → Kafka bootstrap servers (e.g. kafka:29092)
     *   JDBC  → JDBC URL (e.g. jdbc:postgresql://db:5432/mydb)
     */
    private String connection;

    /**
     * Generic destination, interpreted by sink-type:
     *   KAFKA → topic name
     *   JDBC  → table name
     */
    private String destination = "reconciliation-reports";

    // ── JDBC-only credentials ─────────────────────────────────────────────────
    private String username;
    private String password;

    // ── Reconciliation settings ───────────────────────────────────────────────

    /**
     * Aggregation window for counts. Supported units: m (minutes), h (hours), d (days).
     * Examples: "5m", "1h", "1d". Defaults to "1h".
     */
    private String window = "1h";

    /**
     * Acceptable percentage difference between sourceReadCount and targetWrittenCount
     * before the report is flagged as non-reconciled (0-100). Default 0 means exact match.
     */
    private double tolerancePercent = 0.0;

    /**
     * Business-key field names for future record-level reconciliation.
     */
    private List<String> businessKeyFields;

    /** Derives a human-readable window label from a checkpoint interval in milliseconds. */
    public static String windowFromCheckpointInterval(long checkpointIntervalMs) {
        if (checkpointIntervalMs <= 0) return "n/a";
        if (checkpointIntervalMs % 86_400_000L == 0) return (checkpointIntervalMs / 86_400_000L) + "d";
        if (checkpointIntervalMs % 3_600_000L  == 0) return (checkpointIntervalMs / 3_600_000L)  + "h";
        if (checkpointIntervalMs % 60_000L     == 0) return (checkpointIntervalMs / 60_000L)      + "m";
        return (checkpointIntervalMs / 1_000L) + "s";
    }
}
