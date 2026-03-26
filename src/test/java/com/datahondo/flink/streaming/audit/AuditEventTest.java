package com.datahondo.flink.streaming.audit;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AuditEventTest {

    @Test
    void builder_setsAllFields() {
        Instant ts = Instant.parse("2025-01-01T00:00:00Z");
        Map<String, String> meta = Collections.singletonMap("env", "test");

        AuditEvent event = AuditEvent.builder()
                .runId("run-001")
                .jobName("orders-job")
                .eventType(AuditEventType.SOURCE_READ)
                .stage("source/orders")
                .count(500L)
                .timestamp(ts)
                .metadata(meta)
                .build();

        assertEquals("run-001", event.getRunId());
        assertEquals("orders-job", event.getJobName());
        assertEquals(AuditEventType.SOURCE_READ, event.getEventType());
        assertEquals("source/orders", event.getStage());
        assertEquals(500L, event.getCount());
        assertEquals(ts, event.getTimestamp());
        assertEquals("test", event.getMetadata().get("env"));
    }

    @Test
    void builder_defaultCount_isZero() {
        AuditEvent event = AuditEvent.builder()
                .runId("r").jobName("j")
                .eventType(AuditEventType.JOB_SUBMITTED)
                .stage("orchestrator")
                .build();

        assertEquals(0L, event.getCount());
    }

    @Test
    void builder_defaultTimestamp_isNotNull() {
        AuditEvent event = AuditEvent.builder()
                .runId("r").jobName("j")
                .eventType(AuditEventType.JOB_RUNNING)
                .stage("orchestrator")
                .build();

        assertNotNull(event.getTimestamp());
    }

    @Test
    void allEventTypes_areAccessible() {
        // Guards against accidental removal of enum constants
        for (AuditEventType type : AuditEventType.values()) {
            assertNotNull(type.name());
        }
        assertEquals(11, AuditEventType.values().length);
    }
}
