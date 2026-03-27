package com.datahondo.flink.streaming.audit.sink;

import com.datahondo.flink.streaming.audit.AuditEvent;
import com.datahondo.flink.streaming.audit.AuditSink;
import com.datahondo.flink.streaming.config.AuditConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * {@link AuditSink} that inserts each {@link AuditEvent} into a relational table.
 *
 * <p>Expected table DDL:
 * <pre>
 * CREATE TABLE flink_audit_events (
 *     run_id        VARCHAR(128),
 *     job_name      VARCHAR(256),
 *     event_type    VARCHAR(64),
 *     stage         VARCHAR(256),
 *     count         BIGINT,
 *     ts            TIMESTAMP,
 *     metadata      TEXT
 * );
 * </pre>
 */
@Slf4j
public class JdbcAuditSink implements AuditSink {

    private static final String INSERT_SQL =
            "INSERT INTO %s (run_id, job_name, event_type, stage, count, ts, metadata) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)";

    private final AuditConfig config;
    private final ObjectMapper mapper;
    private volatile Connection connection;

    public JdbcAuditSink(AuditConfig config) {
        this.config = config;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void emit(AuditEvent event) {
        try {
            String metadataJson = event.getMetadata() != null
                    ? mapper.writeValueAsString(event.getMetadata()) : null;
            try (PreparedStatement ps = getConnection()
                    .prepareStatement(String.format(INSERT_SQL, config.getDestination()))) {
                ps.setString(1, event.getRunId());
                ps.setString(2, event.getJobName());
                ps.setString(3, event.getEventType() != null ? event.getEventType().name() : null);
                ps.setString(4, event.getStage());
                ps.setLong(5, event.getCount());
                ps.setTimestamp(6, event.getTimestamp() != null
                        ? java.sql.Timestamp.from(event.getTimestamp()) : null);
                ps.setString(7, metadataJson);
                ps.executeUpdate();
            }
        } catch (Exception e) {
            log.warn("[AUDIT-JDBC] Failed to insert audit event: runId={}, error={}",
                    event.getRunId(), e.getMessage());
            connection = null; // force reconnect on next call
        }
    }

    @Override
    public String sinkType() {
        return "JDBC";
    }

    private Connection getConnection() throws Exception {
        if (connection == null || connection.isClosed()) {
            synchronized (this) {
                if (connection == null || connection.isClosed()) {
                    connection = DriverManager.getConnection(
                            config.getConnection(), config.getUsername(), config.getPassword());
                    log.info("[AUDIT-JDBC] Connected to {}, table={}",
                            config.getConnection(), config.getDestination());
                }
            }
        }
        return connection;
    }
}
