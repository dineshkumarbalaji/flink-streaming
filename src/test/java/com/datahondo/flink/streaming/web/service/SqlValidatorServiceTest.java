package com.datahondo.flink.streaming.web.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SqlValidatorService DDL generation and SQL validation.
 */
class SqlValidatorServiceTest {

    private SqlValidatorService service;
    private Method buildDdl;

    @BeforeEach
    void setUp() throws Exception {
        service = new SqlValidatorService();
        buildDdl = SqlValidatorService.class.getDeclaredMethod(
                "buildDdlFromSchema", String.class, String.class, boolean.class, String.class);
        buildDdl.setAccessible(true);
    }

    // ── DDL building ──────────────────────────────────────────────────────────

    @Test
    void buildDdl_containsStringColumn() throws Exception {
        String schema = "{\"properties\":{\"name\":{\"type\":\"string\"}}}";
        String ddl = (String) buildDdl.invoke(service, "t", schema, false, null);
        assertTrue(ddl.contains("`name` STRING"), "DDL: " + ddl);
        assertTrue(ddl.contains("connector"));
    }

    @Test
    void buildDdl_containsIntColumn() throws Exception {
        String schema = "{\"properties\":{\"qty\":{\"type\":\"integer\"}}}";
        String ddl = (String) buildDdl.invoke(service, "t", schema, false, null);
        assertTrue(ddl.contains("`qty` INT"), "DDL: " + ddl);
    }

    @Test
    void buildDdl_containsDoubleColumn() throws Exception {
        String schema = "{\"properties\":{\"price\":{\"type\":\"number\"}}}";
        String ddl = (String) buildDdl.invoke(service, "t", schema, false, null);
        assertTrue(ddl.contains("`price` DOUBLE"), "DDL: " + ddl);
    }

    @Test
    void buildDdl_addsProcTimeColumn_whenWatermarkIsProcessTime() throws Exception {
        String schema = "{\"properties\":{\"id\":{\"type\":\"integer\"}}}";
        String ddl = (String) buildDdl.invoke(service, "t", schema, true, "PROCESS_TIME");
        assertTrue(ddl.contains("processed_time"), "DDL: " + ddl);
        assertTrue(ddl.contains("PROCTIME()"), "DDL: " + ddl);
    }

    @Test
    void buildDdl_throws_whenSchemaHasNoProperties() {
        String schema = "{\"type\":\"object\"}";
        assertThrows(Exception.class, () -> buildDdl.invoke(service, "t", schema, false, null));
    }

    // ── Full SQL validation ───────────────────────────────────────────────────

    @Test
    void validateSql_throws_whenSqlIsEmpty() {
        assertThrows(Exception.class,
                () -> service.validateSql("", "t", null, false, null));
    }

    @Test
    void validateSql_throws_whenSqlIsNull() {
        assertThrows(Exception.class,
                () -> service.validateSql(null, "t", null, false, null));
    }
}
