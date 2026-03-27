package com.datahondo.flink.streaming.audit.sink;

import com.datahondo.flink.streaming.audit.ReconciliationReport;
import com.datahondo.flink.streaming.audit.ReconciliationSink;
import com.datahondo.flink.streaming.config.ReconciliationConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * {@link ReconciliationSink} that inserts each {@link ReconciliationReport} into a relational table.
 *
 * <p>Expected table DDL:
 * <pre>
 * CREATE TABLE flink_reconciliation_reports (
 *     run_id               VARCHAR(128),
 *     job_name             VARCHAR(256),
 *     window_start         TIMESTAMP,
 *     window_end           TIMESTAMP,
 *     window_label         VARCHAR(32),
 *     source_read_count    BIGINT,
 *     schema_rejected_count BIGINT,
 *     transformed_count    BIGINT,
 *     target_written_count BIGINT,
 *     reconciled           BOOLEAN,
 *     discrepancies        TEXT
 * );
 * </pre>
 */
@Slf4j
public class JdbcReconciliationSink implements ReconciliationSink {

    private static final String INSERT_SQL =
            "INSERT INTO %s (run_id, job_name, window_start, window_end, window_label, " +
            "source_read_count, schema_rejected_count, transformed_count, target_written_count, " +
            "reconciled, discrepancies) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final ReconciliationConfig config;
    private final ObjectMapper mapper;
    private volatile Connection connection;

    public JdbcReconciliationSink(ReconciliationConfig config) {
        this.config = config;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void emit(ReconciliationReport report) {
        try {
            String discrepanciesJson = report.getDiscrepancies() != null
                    ? mapper.writeValueAsString(report.getDiscrepancies()) : null;
            try (PreparedStatement ps = getConnection()
                    .prepareStatement(String.format(INSERT_SQL, config.getDestination()))) {
                ps.setString(1, report.getRunId());
                ps.setString(2, report.getJobName());
                ps.setTimestamp(3, report.getWindowStart() != null
                        ? java.sql.Timestamp.from(report.getWindowStart()) : null);
                ps.setTimestamp(4, report.getWindowEnd() != null
                        ? java.sql.Timestamp.from(report.getWindowEnd()) : null);
                ps.setString(5, report.getWindowLabel());
                ps.setLong(6, report.getSourceReadCount());
                ps.setLong(7, report.getSchemaRejectedCount());
                ps.setLong(8, report.getTransformedCount());
                ps.setLong(9, report.getTargetWrittenCount());
                ps.setBoolean(10, report.isReconciled());
                ps.setString(11, discrepanciesJson);
                ps.executeUpdate();
            }
        } catch (Exception e) {
            log.warn("[RECONCILIATION-JDBC] Failed to insert report: runId={}, error={}",
                    report.getRunId(), e.getMessage());
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
                    log.info("[RECONCILIATION-JDBC] Connected to {}, table={}",
                            config.getConnection(), config.getDestination());
                }
            }
        }
        return connection;
    }
}
