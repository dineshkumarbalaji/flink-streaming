package com.datahondo.flink.streaming.audit.sink;

import com.datahondo.flink.streaming.audit.AuditSink;
import com.datahondo.flink.streaming.audit.AuditSinkFactory;
import com.datahondo.flink.streaming.audit.ReconciliationSink;
import com.datahondo.flink.streaming.config.AuditConfig;
import com.datahondo.flink.streaming.config.ReconciliationConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Default {@link AuditSinkFactory} that resolves the correct
 * {@link AuditSink} / {@link ReconciliationSink} implementation from the
 * {@code sinkType} field in the supplied configuration.
 *
 * <p>Supported sink types
 * <table border="1">
 *   <tr><th>sinkType</th><th>AuditSink</th><th>ReconciliationSink</th></tr>
 *   <tr><td>LOG (default)</td><td>{@link LogAuditSink}</td><td>{@link LogReconciliationSink}</td></tr>
 *   <tr><td>KAFKA</td><td>{@link KafkaAuditSink}</td><td>{@link LogReconciliationSink} (fallback)</td></tr>
 *   <tr><td>JDBC</td><td>{@link LogAuditSink} (stub)</td><td>{@link LogReconciliationSink} (stub)</td></tr>
 * </table>
 *
 * <p>JDBC and additional Kafka reconciliation sink implementations are
 * extension points for future iterations.
 */
@Slf4j
@Component
public class DefaultAuditSinkFactory implements AuditSinkFactory {

    @Override
    public AuditSink createAuditSink(AuditConfig config) {
        if (config == null) {
            return new LogAuditSink();
        }
        String type = config.getSinkType() == null ? "LOG" : config.getSinkType().toUpperCase();
        switch (type) {
            case "KAFKA":
                if (config.getConnection() == null || config.getConnection().isEmpty()) {
                    log.warn("[AUDIT-FACTORY] KAFKA sink requested but connection not set — falling back to LOG");
                    return new LogAuditSink();
                }
                log.info("[AUDIT-FACTORY] Creating KafkaAuditSink → topic={}", config.getDestination());
                return new KafkaAuditSink(config);

            case "JDBC":
                if (config.getConnection() == null || config.getConnection().isEmpty()) {
                    log.warn("[AUDIT-FACTORY] JDBC sink requested but connection not set — falling back to LOG");
                    return new LogAuditSink();
                }
                log.info("[AUDIT-FACTORY] Creating JdbcAuditSink → table={}", config.getDestination());
                return new JdbcAuditSink(config);

            case "LOG":
            default:
                return new LogAuditSink();
        }
    }

    @Override
    public ReconciliationSink createReconciliationSink(ReconciliationConfig config) {
        if (config == null) {
            return new LogReconciliationSink();
        }
        String type = config.getSinkType() == null ? "LOG" : config.getSinkType().toUpperCase();
        switch (type) {
            case "KAFKA":
                if (config.getConnection() == null || config.getConnection().isEmpty()) {
                    log.warn("[AUDIT-FACTORY] KAFKA reconciliation sink requested but connection not set — falling back to LOG");
                    return new LogReconciliationSink();
                }
                log.info("[AUDIT-FACTORY] Creating KafkaReconciliationSink → topic={}", config.getDestination());
                return new KafkaReconciliationSink(config);

            case "JDBC":
                if (config.getConnection() == null || config.getConnection().isEmpty()) {
                    log.warn("[AUDIT-FACTORY] JDBC reconciliation sink requested but connection not set — falling back to LOG");
                    return new LogReconciliationSink();
                }
                log.info("[AUDIT-FACTORY] Creating JdbcReconciliationSink → table={}", config.getDestination());
                return new JdbcReconciliationSink(config);

            case "LOG":
            default:
                return new LogReconciliationSink();
        }
    }
}
