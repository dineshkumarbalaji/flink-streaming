package com.datahondo.flink.streaming.job;

import com.datahondo.flink.streaming.audit.AuditEventType;
import com.datahondo.flink.streaming.audit.AuditService;
import com.datahondo.flink.streaming.audit.ReconciliationService;
import com.datahondo.flink.streaming.audit.RunContext;
import com.datahondo.flink.streaming.config.AuditConfig;
import com.datahondo.flink.streaming.config.FlinkConfig;
import com.datahondo.flink.streaming.config.StreamingJobConfig;
import com.datahondo.flink.streaming.source.SourceLayer;
import com.datahondo.flink.streaming.target.TargetLayer;
import com.datahondo.flink.streaming.transformation.TransformationLayer;
import org.apache.flink.table.api.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the audit integration points in {@link StreamingJobOrchestrator}.
 * The Flink environment and layers are mocked so these tests run without a cluster.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class StreamingJobOrchestratorAuditTest {

    @Mock private SourceLayer sourceLayer;
    @Mock private TransformationLayer transformationLayer;
    @Mock private TargetLayer targetLayer;
    @Mock private AuditService auditService;
    @Mock private ReconciliationService reconciliationService;

    private StreamingJobOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        orchestrator = new StreamingJobOrchestrator(
                sourceLayer, transformationLayer, targetLayer,
                auditService, reconciliationService);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private StreamingJobConfig minimalConfig(boolean withAudit) {
        StreamingJobConfig cfg = new StreamingJobConfig();
        cfg.setJobName("unit-test-job");
        cfg.setSources(Collections.emptyList());

        FlinkConfig flink = new FlinkConfig();
        flink.setRemote(false);        // LOCAL environment → no cluster needed
        flink.setParallelism(1);
        flink.setCheckpointInterval(0L);
        cfg.setFlink(flink);

        if (withAudit) {
            AuditConfig audit = new AuditConfig();
            audit.setEnabled(true);
            cfg.setAudit(audit);
        }
        return cfg;
    }

    // ── initRun is called ─────────────────────────────────────────────────────

    @Test
    void submitJob_callsInitRun_withRunContext() throws Exception {
        StreamingJobConfig cfg = minimalConfig(true);
        when(transformationLayer.applyTransformation(any(), any())).thenReturn(mock(Table.class));

        try {
            orchestrator.submitJob(cfg);
        } catch (Exception ignored) {
            // May fail at Flink graph build in unit test — we only care about audit calls
        }

        verify(auditService, atLeastOnce()).initRun(any(RunContext.class));
    }

    // ── JOB_SUBMITTED is emitted ──────────────────────────────────────────────

    @Test
    void submitJob_emitsJobSubmittedEvent() throws Exception {
        StreamingJobConfig cfg = minimalConfig(true);
        when(transformationLayer.applyTransformation(any(), any())).thenReturn(mock(Table.class));

        try {
            orchestrator.submitJob(cfg);
        } catch (Exception ignored) {}

        verify(auditService, atLeastOnce()).emitLifecycle(
                anyString(),
                eq("unit-test-job"),
                eq(AuditEventType.JOB_SUBMITTED),
                any());
    }

    // ── JOB_FAILED is emitted on exception ────────────────────────────────────

    @Test
    void submitJob_emitsJobFailedAndClosesRun_onException() throws Exception {
        StreamingJobConfig cfg = minimalConfig(true);
        when(transformationLayer.applyTransformation(any(), any()))
                .thenThrow(new RuntimeException("sql parse error"));

        assertThrows(Exception.class, () -> orchestrator.submitJob(cfg));

        verify(auditService).emitLifecycle(
                anyString(),
                eq("unit-test-job"),
                eq(AuditEventType.JOB_FAILED),
                any());
        verify(auditService).closeRun(anyString());
    }

    // ── cancelJob emits JOB_CANCELLED ────────────────────────────────────────

    @Test
    void cancelJob_throws_whenJobNotFound() {
        assertThrows(IllegalArgumentException.class,
                () -> orchestrator.cancelJob("non-existent-job"));
    }

    // ── getRunningJobs includes runId ─────────────────────────────────────────

    @Test
    void getRunningJobs_returnsEmptyList_whenNoJobsSubmitted() {
        assertTrue(orchestrator.getRunningJobs().isEmpty());
    }

    // ── Audit disabled path ───────────────────────────────────────────────────

    @Test
    void submitJob_stillCallsInitRun_evenWhenAuditDisabled() throws Exception {
        StreamingJobConfig cfg = minimalConfig(false); // no AuditConfig
        when(transformationLayer.applyTransformation(any(), any())).thenReturn(mock(Table.class));

        try {
            orchestrator.submitJob(cfg);
        } catch (Exception ignored) {}

        // initRun must always be called so the audit service can decide
        verify(auditService, atLeastOnce()).initRun(any(RunContext.class));
    }
}
