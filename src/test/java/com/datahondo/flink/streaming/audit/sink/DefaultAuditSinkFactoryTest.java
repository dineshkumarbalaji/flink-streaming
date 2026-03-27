package com.datahondo.flink.streaming.audit.sink;

import com.datahondo.flink.streaming.audit.AuditSink;
import com.datahondo.flink.streaming.audit.ReconciliationSink;
import com.datahondo.flink.streaming.config.AuditConfig;
import com.datahondo.flink.streaming.config.ReconciliationConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DefaultAuditSinkFactoryTest {

    private DefaultAuditSinkFactory factory;

    @BeforeEach
    void setUp() {
        factory = new DefaultAuditSinkFactory();
    }

    // ── AuditSink ─────────────────────────────────────────────────────────────

    @Test
    void createAuditSink_returnsLog_whenTypeIsLog() {
        AuditConfig cfg = new AuditConfig();
        cfg.setSinkType("LOG");
        AuditSink sink = factory.createAuditSink(cfg);
        assertEquals("LOG", sink.sinkType());
        assertInstanceOf(LogAuditSink.class, sink);
    }

    @Test
    void createAuditSink_returnsLog_whenConfigIsNull() {
        AuditSink sink = factory.createAuditSink(null);
        assertEquals("LOG", sink.sinkType());
    }

    @Test
    void createAuditSink_returnsLog_whenTypeIsUnknown() {
        AuditConfig cfg = new AuditConfig();
        cfg.setSinkType("REDIS");
        AuditSink sink = factory.createAuditSink(cfg);
        assertEquals("LOG", sink.sinkType());
    }

    @Test
    void createAuditSink_returnsLog_whenKafkaRequestedButNoConnection() {
        AuditConfig cfg = new AuditConfig();
        cfg.setSinkType("KAFKA");
        cfg.setConnection(null);
        AuditSink sink = factory.createAuditSink(cfg);
        assertEquals("LOG", sink.sinkType());
    }

    @Test
    void createAuditSink_returnsKafka_whenKafkaFullyConfigured() {
        AuditConfig cfg = new AuditConfig();
        cfg.setSinkType("KAFKA");
        cfg.setConnection("localhost:9092");
        cfg.setDestination("audit");
        AuditSink sink = factory.createAuditSink(cfg);
        assertEquals("KAFKA", sink.sinkType());
        assertInstanceOf(KafkaAuditSink.class, sink);
    }

    @Test
    void createAuditSink_returnsLog_whenJdbcRequestedButNoConnection() {
        AuditConfig cfg = new AuditConfig();
        cfg.setSinkType("JDBC");
        cfg.setConnection(null);
        AuditSink sink = factory.createAuditSink(cfg);
        assertEquals("LOG", sink.sinkType());
    }

    @Test
    void createAuditSink_returnsJdbc_whenJdbcFullyConfigured() {
        AuditConfig cfg = new AuditConfig();
        cfg.setSinkType("JDBC");
        cfg.setConnection("jdbc:postgresql://db:5432/mydb");
        cfg.setDestination("flink_audit_events");
        cfg.setUsername("user");
        cfg.setPassword("pass");
        AuditSink sink = factory.createAuditSink(cfg);
        assertEquals("JDBC", sink.sinkType());
        assertInstanceOf(JdbcAuditSink.class, sink);
    }

    // ── ReconciliationSink ────────────────────────────────────────────────────

    @Test
    void createReconciliationSink_returnsLog_whenTypeIsLog() {
        ReconciliationConfig cfg = new ReconciliationConfig();
        cfg.setSinkType("LOG");
        ReconciliationSink sink = factory.createReconciliationSink(cfg);
        assertEquals("LOG", sink.sinkType());
        assertInstanceOf(LogReconciliationSink.class, sink);
    }

    @Test
    void createReconciliationSink_returnsLog_whenConfigIsNull() {
        ReconciliationSink sink = factory.createReconciliationSink(null);
        assertEquals("LOG", sink.sinkType());
    }

    @Test
    void createReconciliationSink_returnsLog_whenKafkaRequestedButNoConnection() {
        ReconciliationConfig cfg = new ReconciliationConfig();
        cfg.setSinkType("KAFKA");
        cfg.setConnection(null);
        ReconciliationSink sink = factory.createReconciliationSink(cfg);
        assertEquals("LOG", sink.sinkType());
    }

    @Test
    void createReconciliationSink_returnsKafka_whenKafkaFullyConfigured() {
        ReconciliationConfig cfg = new ReconciliationConfig();
        cfg.setSinkType("KAFKA");
        cfg.setConnection("localhost:9092");
        cfg.setDestination("reconciliation-reports");
        ReconciliationSink sink = factory.createReconciliationSink(cfg);
        assertEquals("KAFKA", sink.sinkType());
        assertInstanceOf(KafkaReconciliationSink.class, sink);
    }

    @Test
    void createReconciliationSink_returnsLog_whenJdbcRequestedButNoConnection() {
        ReconciliationConfig cfg = new ReconciliationConfig();
        cfg.setSinkType("JDBC");
        cfg.setConnection(null);
        ReconciliationSink sink = factory.createReconciliationSink(cfg);
        assertEquals("LOG", sink.sinkType());
    }

    @Test
    void createReconciliationSink_returnsJdbc_whenJdbcFullyConfigured() {
        ReconciliationConfig cfg = new ReconciliationConfig();
        cfg.setSinkType("JDBC");
        cfg.setConnection("jdbc:postgresql://db:5432/mydb");
        cfg.setDestination("flink_reconciliation_reports");
        cfg.setUsername("user");
        cfg.setPassword("pass");
        ReconciliationSink sink = factory.createReconciliationSink(cfg);
        assertEquals("JDBC", sink.sinkType());
        assertInstanceOf(JdbcReconciliationSink.class, sink);
    }
}
