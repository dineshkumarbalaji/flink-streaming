package com.datahondo.flink.streaming.audit;

import com.datahondo.flink.streaming.config.AuditConfig;
import com.datahondo.flink.streaming.config.ReconciliationConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RunContextTest {

    @Test
    void create_generatesUniqueRunIds() {
        RunContext a = RunContext.create("job", null, null);
        RunContext b = RunContext.create("job", null, null);
        assertNotEquals(a.getRunId(), b.getRunId());
    }

    @Test
    void create_runId_containsJobName() {
        RunContext ctx = RunContext.create("my-job", null, null);
        assertTrue(ctx.getRunId().startsWith("my-job"),
                "runId should start with jobName: " + ctx.getRunId());
    }

    @Test
    void create_runId_sanitizesSpecialChars() {
        RunContext ctx = RunContext.create("job with spaces!", null, null);
        assertFalse(ctx.getRunId().contains(" "), "runId must not contain spaces");
        assertFalse(ctx.getRunId().contains("!"), "runId must not contain '!'");
    }

    @Test
    void isAuditEnabled_falseWhenConfigNull() {
        RunContext ctx = RunContext.create("j", null, null);
        assertFalse(ctx.isAuditEnabled());
    }

    @Test
    void isAuditEnabled_falseWhenConfigDisabled() {
        AuditConfig cfg = new AuditConfig();
        cfg.setEnabled(false);
        RunContext ctx = RunContext.create("j", cfg, null);
        assertFalse(ctx.isAuditEnabled());
    }

    @Test
    void isAuditEnabled_trueWhenConfigEnabled() {
        AuditConfig cfg = new AuditConfig();
        cfg.setEnabled(true);
        RunContext ctx = RunContext.create("j", cfg, null);
        assertTrue(ctx.isAuditEnabled());
    }

    @Test
    void isReconciliationEnabled_trueWhenConfigEnabled() {
        ReconciliationConfig cfg = new ReconciliationConfig();
        cfg.setEnabled(true);
        RunContext ctx = RunContext.create("j", null, cfg);
        assertTrue(ctx.isReconciliationEnabled());
    }

    @Test
    void create_setsStartTime() {
        RunContext ctx = RunContext.create("j", null, null);
        assertNotNull(ctx.getStartTime());
    }
}
