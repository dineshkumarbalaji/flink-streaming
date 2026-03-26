package com.datahondo.flink.streaming.audit.sink;

import com.datahondo.flink.streaming.audit.ReconciliationReport;
import com.datahondo.flink.streaming.audit.ReconciliationSink;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

/**
 * Zero-dependency {@link ReconciliationSink} that writes each
 * {@link ReconciliationReport} as structured JSON via SLF4J at INFO level.
 *
 * <p>Non-reconciled reports are additionally logged at WARN level so they
 * surface in dashboards and alerting pipelines without extra configuration.
 */
@Slf4j
public class LogReconciliationSink implements ReconciliationSink {

    private final ObjectMapper mapper;

    public LogReconciliationSink() {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void emit(ReconciliationReport report) {
        try {
            String json = mapper.writeValueAsString(report);
            if (report.isReconciled()) {
                log.info("[RECONCILIATION] PASS {}", json);
            } else {
                log.warn("[RECONCILIATION] FAIL discrepancies={} {}",
                        report.getDiscrepancies(), json);
            }
        } catch (Exception e) {
            log.warn("[RECONCILIATION] Failed to serialise report: runId={}, error={}",
                    report.getRunId(), e.getMessage());
        }
    }

    @Override
    public String sinkType() {
        return "LOG";
    }
}
