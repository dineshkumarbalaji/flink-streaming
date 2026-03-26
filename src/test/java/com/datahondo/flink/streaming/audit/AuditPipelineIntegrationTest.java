package com.datahondo.flink.streaming.audit;

import com.datahondo.flink.streaming.audit.sink.DefaultAuditSinkFactory;
import com.datahondo.flink.streaming.config.AuditConfig;
import com.datahondo.flink.streaming.config.ReconciliationConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test: wires AuditService + ReconciliationService together
 * (no Spring context, no Flink cluster) and simulates a complete job lifecycle
 * with a custom in-memory AuditSink for assertion.
 */
class AuditPipelineIntegrationTest {

    // ── Spy sink ──────────────────────────────────────────────────────────────

    static class CapturingAuditSink implements AuditSink {
        final List<AuditEvent> events = new ArrayList<>();
        @Override public void emit(AuditEvent event) { events.add(event); }
        @Override public String sinkType() { return "CAPTURE"; }
    }

    static class CapturingReconSink implements ReconciliationSink {
        final List<ReconciliationReport> reports = new ArrayList<>();
        @Override public void emit(ReconciliationReport r) { reports.add(r); }
        @Override public String sinkType() { return "CAPTURE"; }
    }

    // ── Stub factory ──────────────────────────────────────────────────────────

    private CapturingAuditSink captureSink;
    private CapturingReconSink captureReconSink;
    private AuditService auditService;
    private ReconciliationService reconciliationService;

    @BeforeEach
    void setUp() {
        captureSink      = new CapturingAuditSink();
        captureReconSink = new CapturingReconSink();

        AuditSinkFactory factory = new DefaultAuditSinkFactory() {
            @Override
            public AuditSink createAuditSink(AuditConfig cfg) { return captureSink; }
            @Override
            public ReconciliationSink createReconciliationSink(ReconciliationConfig cfg) {
                return captureReconSink;
            }
        };

        auditService          = new AuditService(factory);
        reconciliationService = new ReconciliationService(factory);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void fullLifecycle_emitsAllExpectedAuditEvents() {
        AuditConfig auditCfg = new AuditConfig();
        auditCfg.setEnabled(true);
        ReconciliationConfig reconCfg = new ReconciliationConfig();
        reconCfg.setEnabled(true);

        RunContext ctx = RunContext.create("orders-job", auditCfg, reconCfg);
        auditService.initRun(ctx);

        // Simulate pipeline events
        auditService.emitLifecycle(ctx.getRunId(), ctx.getJobName(), AuditEventType.JOB_SUBMITTED, null);
        auditService.emitLifecycle(ctx.getRunId(), ctx.getJobName(), AuditEventType.JOB_RUNNING, null);
        auditService.emitCount(ctx.getRunId(), ctx.getJobName(), AuditEventType.SOURCE_READ, "source/orders", 1000L, null);
        auditService.emitCount(ctx.getRunId(), ctx.getJobName(), AuditEventType.SOURCE_REJECTED, "source/orders", 5L, null);
        auditService.emitCount(ctx.getRunId(), ctx.getJobName(), AuditEventType.TRANSFORM_OUTPUT, "transformation", 995L, null);
        auditService.emitCount(ctx.getRunId(), ctx.getJobName(), AuditEventType.TARGET_WRITTEN, "target/enriched", 995L, null);
        auditService.emitLifecycle(ctx.getRunId(), ctx.getJobName(), AuditEventType.JOB_COMPLETED, null);
        auditService.closeRun(ctx.getRunId());

        assertEquals(7, captureSink.events.size(), "Expected 7 audit events");

        // Verify ordering reflects pipeline stages
        assertEquals(AuditEventType.JOB_SUBMITTED, captureSink.events.get(0).getEventType());
        assertEquals(AuditEventType.JOB_COMPLETED, captureSink.events.get(6).getEventType());

        // All events share the same runId
        String runId = ctx.getRunId();
        assertTrue(captureSink.events.stream().allMatch(e -> runId.equals(e.getRunId())));
    }

    @Test
    void reconciliation_producesPassReport_whenCountsMatch() {
        ReconciliationConfig reconCfg = new ReconciliationConfig();
        reconCfg.setEnabled(true);
        RunContext ctx = RunContext.create("clean-job", null, reconCfg);

        ReconciliationService.Counts counts = ReconciliationService.Counts.of(500, 0, 500, 500);
        ReconciliationReport report = reconciliationService.reconcile(ctx, Instant.now().minusSeconds(3600), counts);

        assertTrue(report.isReconciled());
        assertEquals(1, captureReconSink.reports.size());
        assertEquals(report.getRunId(), captureReconSink.reports.get(0).getRunId());
    }

    @Test
    void reconciliation_producesFailReport_andDescribesDiscrepancy() {
        ReconciliationConfig reconCfg = new ReconciliationConfig();
        reconCfg.setEnabled(true);
        RunContext ctx = RunContext.create("leaky-job", null, reconCfg);

        // 20 records lost at target
        ReconciliationService.Counts counts = ReconciliationService.Counts.of(100, 0, 100, 80);
        ReconciliationReport report = reconciliationService.reconcile(ctx, Instant.now().minusSeconds(300), counts);

        assertFalse(report.isReconciled());
        assertFalse(report.getDiscrepancies().isEmpty());
        assertEquals(1, captureReconSink.reports.size());
    }

    @Test
    void auditService_activeRunCount_isZeroAfterClose() {
        AuditConfig cfg = new AuditConfig();
        cfg.setEnabled(true);
        RunContext ctx = RunContext.create("temp-job", cfg, null);

        auditService.initRun(ctx);
        assertEquals(1, auditService.activeRunCount());

        auditService.closeRun(ctx.getRunId());
        assertEquals(0, auditService.activeRunCount());
    }
}
