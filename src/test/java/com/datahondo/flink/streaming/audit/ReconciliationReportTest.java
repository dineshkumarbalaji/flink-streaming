package com.datahondo.flink.streaming.audit;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ReconciliationReportTest {

    private ReconciliationReport.ReconciliationReportBuilder base() {
        return ReconciliationReport.builder()
                .runId("r-1")
                .jobName("job")
                .windowStart(Instant.now().minusSeconds(3600))
                .windowEnd(Instant.now())
                .windowLabel("1h");
    }

    @Test
    void getNetInputCount_subtractsRejected() {
        ReconciliationReport report = base()
                .sourceReadCount(1000).schemaRejectedCount(50)
                .transformedCount(950).targetWrittenCount(950)
                .reconciled(true).build();

        assertEquals(950L, report.getNetInputCount());
    }

    @Test
    void getTargetLag_isZeroWhenCounstMatch() {
        ReconciliationReport report = base()
                .sourceReadCount(100).schemaRejectedCount(0)
                .transformedCount(100).targetWrittenCount(100)
                .reconciled(true).build();

        assertEquals(0L, report.getTargetLag());
    }

    @Test
    void getTargetLag_isPositiveWhenDataLost() {
        ReconciliationReport report = base()
                .sourceReadCount(100).schemaRejectedCount(0)
                .transformedCount(100).targetWrittenCount(90)
                .reconciled(false).build();

        assertEquals(10L, report.getTargetLag());
    }

    @Test
    void toString_containsKeyFields() {
        ReconciliationReport report = base()
                .sourceReadCount(5).schemaRejectedCount(1)
                .transformedCount(4).targetWrittenCount(4)
                .reconciled(true).build();

        String s = report.toString();
        assertTrue(s.contains("r-1"));
        assertTrue(s.contains("job"));
        assertTrue(s.contains("reconciled=true"));
    }

    @Test
    void defaultDiscrepancies_isEmpty() {
        ReconciliationReport report = base()
                .sourceReadCount(0).schemaRejectedCount(0)
                .transformedCount(0).targetWrittenCount(0)
                .reconciled(true).build();

        assertNotNull(report.getDiscrepancies());
        assertTrue(report.getDiscrepancies().isEmpty());
    }
}
