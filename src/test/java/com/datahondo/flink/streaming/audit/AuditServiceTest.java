package com.datahondo.flink.streaming.audit;

import com.datahondo.flink.streaming.config.AuditConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class AuditServiceTest {

    @Mock
    private AuditSinkFactory sinkFactory;

    @Mock
    private AuditSink mockSink;

    private AuditService service;

    @BeforeEach
    void setUp() {
        when(sinkFactory.createAuditSink(any())).thenReturn(mockSink);
        when(mockSink.sinkType()).thenReturn("MOCK");
        service = new AuditService(sinkFactory);
    }

    private RunContext enabledRun(String jobName) {
        AuditConfig cfg = new AuditConfig();
        cfg.setEnabled(true);
        return RunContext.create(jobName, cfg, null);
    }

    // ── initRun ───────────────────────────────────────────────────────────────

    @Test
    void initRun_createsSink_whenAuditEnabled() {
        RunContext ctx = enabledRun("job");
        service.initRun(ctx);
        verify(sinkFactory).createAuditSink(ctx.getAuditConfig());
        assertEquals(1, service.activeRunCount());
    }

    @Test
    void initRun_doesNotCreateSink_whenAuditDisabled() {
        RunContext ctx = RunContext.create("job", null, null);
        service.initRun(ctx);
        verifyNoInteractions(sinkFactory);
        assertEquals(0, service.activeRunCount());
    }

    // ── emit ──────────────────────────────────────────────────────────────────

    @Test
    void emit_delegatesToSink_afterInit() {
        RunContext ctx = enabledRun("job");
        service.initRun(ctx);

        AuditEvent event = AuditEvent.builder()
                .runId(ctx.getRunId())
                .jobName("job")
                .eventType(AuditEventType.SOURCE_READ)
                .stage("source/t")
                .count(42L)
                .build();

        service.emit(event);

        ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(mockSink).emit(captor.capture());
        assertEquals(42L, captor.getValue().getCount());
    }

    @Test
    void emit_isNoOp_whenRunNotInitialised() {
        AuditEvent event = AuditEvent.builder()
                .runId("unknown-run").jobName("j")
                .eventType(AuditEventType.JOB_SUBMITTED)
                .stage("orchestrator").build();

        // Should not throw; mockSink should not be called
        service.emit(event);
        verifyNoInteractions(mockSink);
    }

    @Test
    void emit_isNoOp_forNullEvent() {
        assertDoesNotThrow(() -> service.emit(null));
    }

    // ── closeRun ──────────────────────────────────────────────────────────────

    @Test
    void closeRun_removesActiveSink() {
        RunContext ctx = enabledRun("job");
        service.initRun(ctx);
        assertEquals(1, service.activeRunCount());

        service.closeRun(ctx.getRunId());
        assertEquals(0, service.activeRunCount());
    }

    @Test
    void closeRun_doesNotThrow_forUnknownRunId() {
        assertDoesNotThrow(() -> service.closeRun("no-such-run"));
    }

    // ── emitCount convenience ─────────────────────────────────────────────────

    @Test
    void emitCount_buildsAndEmitsEvent() {
        RunContext ctx = enabledRun("job");
        service.initRun(ctx);

        service.emitCount(ctx.getRunId(), "job",
                AuditEventType.TARGET_WRITTEN, "target/out", 99L, null);

        ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(mockSink).emit(captor.capture());
        assertEquals(AuditEventType.TARGET_WRITTEN, captor.getValue().getEventType());
        assertEquals(99L, captor.getValue().getCount());
    }

    // ── emitLifecycle ─────────────────────────────────────────────────────────

    @Test
    void emitLifecycle_emitsZeroCountEvent() {
        RunContext ctx = enabledRun("job");
        service.initRun(ctx);

        service.emitLifecycle(ctx.getRunId(), "job",
                AuditEventType.JOB_COMPLETED, Collections.singletonMap("k", "v"));

        ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(mockSink).emit(captor.capture());
        assertEquals(0L, captor.getValue().getCount());
        assertEquals(AuditEventType.JOB_COMPLETED, captor.getValue().getEventType());
    }

    // ── sink exception isolation ──────────────────────────────────────────────

    @Test
    void emit_doesNotPropagate_sinkException() {
        RunContext ctx = enabledRun("job");
        service.initRun(ctx);
        doThrow(new RuntimeException("sink down")).when(mockSink).emit(any());

        AuditEvent event = AuditEvent.builder()
                .runId(ctx.getRunId()).jobName("j")
                .eventType(AuditEventType.SOURCE_READ)
                .stage("s").build();

        assertDoesNotThrow(() -> service.emit(event));
    }
}
