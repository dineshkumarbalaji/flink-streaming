package com.datahondo.flink.streaming.config;

import lombok.Data;

/**
 * Controls how job-level audit events are captured and persisted.
 *
 * Supported sink types:
 *   LOG   – SLF4J structured log line (default, zero external dependencies)
 *   KAFKA – connection = bootstrap-servers, destination = topic
 *   JDBC  – connection = jdbc-url,          destination = table name
 */
@Data
public class AuditConfig {

    /** Master switch. When false all audit emission is skipped. */
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
    private String destination = "audit-events";

    // ── JDBC-only credentials ─────────────────────────────────────────────────
    private String username;
    private String password;

    // ── Kafka-only auth ───────────────────────────────────────────────────────
    private AuthConfig kafkaAuth;

    // ── Metadata ──────────────────────────────────────────────────────────────
    /** Free-form tags attached to every audit event emitted by this job. */
    private java.util.Map<String, String> tags;
}
