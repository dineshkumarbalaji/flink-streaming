package com.datahondo.flink.streaming.audit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class AuditAccumulatorsTest {

    @Test
    void sourceRead_formatsCorrectly() {
        assertEquals("audit.source.orders_topic.read",
                AuditAccumulators.sourceRead("orders_topic"));
    }

    @Test
    void sourceRejected_formatsCorrectly() {
        assertEquals("audit.source.orders_topic.rejected",
                AuditAccumulators.sourceRejected("orders_topic"));
    }

    @Test
    void targetWritten_formatsCorrectly() {
        assertEquals("audit.target.output_topic.written",
                AuditAccumulators.targetWritten("output_topic"));
    }

    @Test
    void sourceRead_sanitizesSpecialChars() {
        String key = AuditAccumulators.sourceRead("my-topic.v2");
        assertFalse(key.contains("-"), "Hyphens should be sanitised: " + key);
        // Structural dots (e.g. "audit.source.X.read") are expected — only the table-name
        // portion between "source." and ".read" must have dots sanitised.
        String tableNamePart = key.replace("audit.source.", "").replace(".read", "");
        assertFalse(tableNamePart.contains("."),
                "Dots in the table-name segment should be sanitised, got: " + tableNamePart);
    }

    @Test
    void sourceRead_handlesNull() {
        assertDoesNotThrow(() -> AuditAccumulators.sourceRead(null));
        assertTrue(AuditAccumulators.sourceRead(null).contains("unknown"));
    }

    @Test
    void aggregatedConstants_haveExpectedFormat() {
        assertTrue(AuditAccumulators.SOURCE_READ_ALL.startsWith("audit."));
        assertTrue(AuditAccumulators.SOURCE_REJECTED_ALL.startsWith("audit."));
        assertTrue(AuditAccumulators.TARGET_WRITTEN_ALL.startsWith("audit."));
        assertTrue(AuditAccumulators.TRANSFORM_OUT.startsWith("audit."));
    }

    @ParameterizedTest
    @ValueSource(strings = {"my topic", "topic/v1", "topic@2024"})
    void sourceRead_neverContainsWhitespaceOrSlash(String tableName) {
        String key = AuditAccumulators.sourceRead(tableName);
        assertFalse(key.contains(" "));
        assertFalse(key.contains("/"));
    }
}
