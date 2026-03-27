package com.datahondo.flink.streaming.audit;

import com.datahondo.flink.streaming.audit.sink.DefaultAuditSinkFactory;
import com.datahondo.flink.streaming.config.ReconciliationConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReconciliationServiceTest {

    private ReconciliationService service;

    @BeforeEach
    void setUp() {
        service = new ReconciliationService(new DefaultAuditSinkFactory());
    }

    private RunContext reconcileCtx() {
        ReconciliationConfig cfg = new ReconciliationConfig();
        cfg.setEnabled(true);
        cfg.setWindow("1h");
        cfg.setTolerancePercent(0.0);
        return RunContext.create("test-job", null, cfg);
    }

    // ── Happy path ────────────────────────────────────────────────────────────

    @Test
    void reconcile_isReconciled_whenCountsMatch() {
        ReconciliationService.Counts counts = ReconciliationService.Counts.of(100, 0, 100, 100);
        ReconciliationReport report = service.reconcile(reconcileCtx(), Instant.now(), counts);

        assertTrue(report.isReconciled());
        assertTrue(report.getDiscrepancies().isEmpty());
        assertEquals(100, report.getSourceReadCount());
        assertEquals(100, report.getTargetWrittenCount());
    }

    // ── Rejection discrepancy ─────────────────────────────────────────────────

    @Test
    void reconcile_notReconciled_whenRejectionsExist() {
        ReconciliationService.Counts counts = ReconciliationService.Counts.of(100, 5, 95, 95);
        ReconciliationReport report = service.reconcile(reconcileCtx(), Instant.now(), counts);

        assertFalse(report.isReconciled());
        assertFalse(report.getDiscrepancies().isEmpty());
        assertTrue(report.getDiscrepancies().get(0).contains("rejected"));
    }

    // ── Target lag ────────────────────────────────────────────────────────────

    @Test
    void reconcile_notReconciled_whenTargetLag() {
        ReconciliationService.Counts counts = ReconciliationService.Counts.of(100, 0, 100, 90);
        ReconciliationReport report = service.reconcile(reconcileCtx(), Instant.now(), counts);

        assertFalse(report.isReconciled());
        assertTrue(report.getDiscrepancies().stream()
                .anyMatch(d -> d.contains("90") && d.contains("100")));
    }

    // ── Tolerance ─────────────────────────────────────────────────────────────

    @Test
    void reconcile_isReconciled_whenLagWithinTolerance() {
        ReconciliationConfig cfg = new ReconciliationConfig();
        cfg.setEnabled(true);
        cfg.setTolerancePercent(5.0); // 5% tolerance
        RunContext ctx = RunContext.create("j", null, cfg);

        // 3% loss — within tolerance
        ReconciliationService.Counts counts = ReconciliationService.Counts.of(100, 0, 100, 97);
        ReconciliationReport report = service.reconcile(ctx, Instant.now(), counts);

        assertTrue(report.isReconciled(),
                "3% loss should pass with 5% tolerance: " + report.getDiscrepancies());
    }

    @Test
    void reconcile_notReconciled_whenLagExceedsTolerance() {
        ReconciliationConfig cfg = new ReconciliationConfig();
        cfg.setEnabled(true);
        cfg.setTolerancePercent(5.0);
        RunContext ctx = RunContext.create("j", null, cfg);

        // 10% loss — exceeds tolerance
        ReconciliationService.Counts counts = ReconciliationService.Counts.of(100, 0, 100, 90);
        ReconciliationReport report = service.reconcile(ctx, Instant.now(), counts);

        assertFalse(report.isReconciled());
    }

    // ── Duplication detection ─────────────────────────────────────────────────

    @Test
    void reconcile_notReconciled_whenTargetWrittenExceedsTransformed() {
        ReconciliationService.Counts counts = ReconciliationService.Counts.of(100, 0, 100, 110);
        ReconciliationReport report = service.reconcile(reconcileCtx(), Instant.now(), counts);

        assertFalse(report.isReconciled());
        assertTrue(report.getDiscrepancies().stream()
                .anyMatch(d -> d.toLowerCase().contains("duplic")));
    }

    // ── Accumulator extraction ────────────────────────────────────────────────

    @Test
    void countsFromAccumulators_extractsLongValues() {
        Map<String, Object> acc = new HashMap<>();
        acc.put("audit.source.all.read", 200L);
        acc.put("audit.source.all.rejected", 10L);
        acc.put(AuditAccumulators.TRANSFORM_OUT, 190L);
        acc.put("audit.target.all.written", 188L);

        ReconciliationService.Counts counts = ReconciliationService.countsFromAccumulators(
                acc,
                AuditAccumulators.SOURCE_READ_ALL,
                AuditAccumulators.SOURCE_REJECTED_ALL,
                AuditAccumulators.TARGET_WRITTEN_ALL);

        assertEquals(200L, counts.sourceRead);
        assertEquals(10L,  counts.schemaRejected);
        assertEquals(190L, counts.transformed);
        assertEquals(188L, counts.targetWritten);
    }

    @Test
    void countsFromAccumulators_defaultsToZero_forMissingKeys() {
        ReconciliationService.Counts counts = ReconciliationService.countsFromAccumulators(
                new HashMap<>(), "missing-a", "missing-b", "missing-c");

        assertEquals(0L, counts.sourceRead);
        assertEquals(0L, counts.schemaRejected);
        assertEquals(0L, counts.targetWritten);
    }

    // ── Window metadata ───────────────────────────────────────────────────────

    @Test
    void reconcile_setsWindowStartAndEnd() {
        Instant start = Instant.now().minusSeconds(60);
        ReconciliationService.Counts counts = ReconciliationService.Counts.of(10, 0, 10, 10);
        ReconciliationReport report = service.reconcile(reconcileCtx(), start, counts);

        assertEquals(start, report.getWindowStart());
        assertNotNull(report.getWindowEnd());
        assertTrue(report.getWindowEnd().isAfter(start));
    }
}
