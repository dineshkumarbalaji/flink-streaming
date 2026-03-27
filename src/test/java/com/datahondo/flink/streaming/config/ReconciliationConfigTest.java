package com.datahondo.flink.streaming.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class ReconciliationConfigTest {

    @ParameterizedTest(name = "{0} ms → \"{1}\"")
    @CsvSource({
        "300000,   5m",
        "3600000,  1h",
        "86400000, 1d",
        "7200000,  2h",
        "1800000,  30m",
        "90000,    90s"
    })
    void windowFromCheckpointInterval_formatsCorrectly(long ms, String expected) {
        assertEquals(expected.trim(), ReconciliationConfig.windowFromCheckpointInterval(ms));
    }

    @Test
    void windowFromCheckpointInterval_returnsNa_whenZeroOrNegative() {
        assertEquals("n/a", ReconciliationConfig.windowFromCheckpointInterval(0));
        assertEquals("n/a", ReconciliationConfig.windowFromCheckpointInterval(-1));
    }

    @Test
    void defaults_areReasonable() {
        ReconciliationConfig cfg = new ReconciliationConfig();
        assertTrue(cfg.isEnabled());
        assertEquals("1h", cfg.getWindow());
        assertEquals("LOG", cfg.getSinkType());
        assertEquals(0.0, cfg.getTolerancePercent());
    }
}
