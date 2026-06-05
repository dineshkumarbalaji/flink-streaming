package com.datahondo.flink.streaming.sink;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class JdbcDialectTest {

    @Test
    void detect_returnsPostgresql_forPostgresUrl() {
        assertEquals(JdbcDialect.POSTGRESQL, JdbcDialect.detect("jdbc:postgresql://host:5432/db"));
    }

    @Test
    void detect_returnsMysql_forMysqlUrl() {
        assertEquals(JdbcDialect.MYSQL, JdbcDialect.detect("jdbc:mysql://host:3306/db"));
    }

    @Test
    void detect_returnsOracle_forOracleUrl() {
        assertEquals(JdbcDialect.ORACLE, JdbcDialect.detect("jdbc:oracle:thin:@host:1521/db"));
    }

    @Test
    void detect_returnsPostgresql_forNullUrl() {
        assertEquals(JdbcDialect.POSTGRESQL, JdbcDialect.detect(null));
    }

    @Test
    void postgresDialect_generatesCorrectInsertSql() {
        List<String> cols = Arrays.asList("id", "name", "amount");
        String sql = JdbcDialect.POSTGRESQL.insertSql("orders", cols);
        assertEquals("INSERT INTO orders (id, name, amount) VALUES (?, ?, ?)", sql);
    }

    @Test
    void postgresDialect_generatesCorrectUpsertSql() {
        List<String> cols = Arrays.asList("id", "name", "amount");
        List<String> keys = Collections.singletonList("id");
        String sql = JdbcDialect.POSTGRESQL.upsertSql("orders", cols, keys);
        assertTrue(sql.contains("ON CONFLICT (id) DO UPDATE SET"));
        assertTrue(sql.contains("name=EXCLUDED.name"));
        assertTrue(sql.contains("amount=EXCLUDED.amount"));
    }

    @Test
    void mysqlDialect_generatesCorrectUpsertSql() {
        List<String> cols = Arrays.asList("id", "name");
        List<String> keys = Collections.singletonList("id");
        String sql = JdbcDialect.MYSQL.upsertSql("orders", cols, keys);
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
        assertTrue(sql.contains("name=VALUES(name)"));
    }

    @Test
    void insertSql_hasCorrectPlaceholderCount() {
        List<String> cols = Arrays.asList("a", "b", "c", "d");
        String sql = JdbcDialect.POSTGRESQL.insertSql("t", cols);
        long count = sql.chars().filter(c -> c == '?').count();
        assertEquals(4, count);
    }
}
