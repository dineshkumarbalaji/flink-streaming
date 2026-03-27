package com.datahondo.flink.streaming.audit.sink;

import com.datahondo.flink.streaming.audit.AuditEvent;
import com.datahondo.flink.streaming.audit.AuditSink;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

/**
 * Zero-dependency {@link AuditSink} that serialises each {@link AuditEvent}
 * to a single-line JSON string and writes it via SLF4J at INFO level.
 *
 * <p>This is the default sink and requires no external infrastructure.
 * Log aggregators (ELK, Splunk, Datadog) can ingest the structured JSON lines.
 *
 * <p>Thread-safe: {@link ObjectMapper} is reused (it is thread-safe after configuration).
 */
@Slf4j
public class LogAuditSink implements AuditSink {

    private final ObjectMapper mapper;

    public LogAuditSink() {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void emit(AuditEvent event) {
        try {
            String json = mapper.writeValueAsString(event);
            log.info("[AUDIT] {}", json);
        } catch (Exception e) {
            // Audit must never break the pipeline — degrade gracefully
            log.warn("[AUDIT] Failed to serialise audit event: eventType={}, runId={}, error={}",
                    event.getEventType(), event.getRunId(), e.getMessage());
        }
    }

    @Override
    public String sinkType() {
        return "LOG";
    }
}
