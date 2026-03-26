package com.datahondo.flink.streaming.audit.sink;

import com.datahondo.flink.streaming.audit.AuditEvent;
import com.datahondo.flink.streaming.audit.AuditEventType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that LogAuditSink never throws and always returns "LOG" as its type token.
 * We cannot assert exact log output in a unit test without a log-capturing framework,
 * so we focus on contract guarantees (no exception, correct sinkType).
 */
class LogAuditSinkTest {

    private final LogAuditSink sink = new LogAuditSink();

    @Test
    void sinkType_returnsLog() {
        assertEquals("LOG", sink.sinkType());
    }

    @Test
    void emit_doesNotThrow_forMinimalEvent() {
        AuditEvent event = AuditEvent.builder()
                .runId("r").jobName("j")
                .eventType(AuditEventType.JOB_SUBMITTED)
                .stage("orchestrator")
                .build();

        assertDoesNotThrow(() -> sink.emit(event));
    }

    @Test
    void emit_doesNotThrow_forFullEvent() {
        AuditEvent event = AuditEvent.builder()
                .runId("run-xyz")
                .jobName("orders-enrichment")
                .eventType(AuditEventType.SOURCE_READ)
                .stage("source/orders-topic")
                .count(1_234_567L)
                .timestamp(Instant.now())
                .metadata(new HashMap<String, String>() {{ put("env", "prod"); put("region", "us-east-1"); }})
                .build();

        assertDoesNotThrow(() -> sink.emit(event));
    }

    @Test
    void emit_doesNotThrow_forEventWithNullMetadata() {
        AuditEvent event = AuditEvent.builder()
                .runId("r").jobName("j")
                .eventType(AuditEventType.TARGET_WRITTEN)
                .stage("target/out")
                .count(100L)
                .metadata(null)
                .build();

        assertDoesNotThrow(() -> sink.emit(event));
    }

    @Test
    void emit_isIdempotent_calledMultipleTimes() {
        AuditEvent event = AuditEvent.builder()
                .runId("r").jobName("j")
                .eventType(AuditEventType.RECONCILIATION_COMPLETE)
                .stage("orchestrator")
                .build();

        assertDoesNotThrow(() -> {
            for (int i = 0; i < 10; i++) sink.emit(event);
        });
    }
}
