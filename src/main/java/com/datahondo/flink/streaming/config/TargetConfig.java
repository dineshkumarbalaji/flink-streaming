package com.datahondo.flink.streaming.config;

import lombok.Data;
import java.util.List;

/**
 * Per-target (sink) configuration. The {@code type} field selects the sink layer:
 * KAFKA (default) | JDBC | FILE | API
 */
@Data
public class TargetConfig {

    /** Sink type discriminator. Defaults to KAFKA for backward compatibility. */
    private String type = "KAFKA";

    // ── Common ────────────────────────────────────────────────────────────────
    private KafkaConfig kafka;
    private SchemaConfig schema;

    // ── JDBC sink (010-A) ─────────────────────────────────────────────────────
    private String jdbcUrl;
    private String tableName;
    private int batchSize = 500;
    private long batchIntervalMs = 1000;
    /** When true, generates upsert SQL instead of plain INSERT. */
    private boolean upsertMode = false;
    /**
     * Explicit primary key column list for upsert. If empty, falls back to
     * schema fields with primaryKey=true.
     */
    private List<String> upsertKeyColumns;
    private String jdbcUsername;
    private String jdbcPassword;
    /** disable | require | verify-full */
    private String sslMode;
    /** POSTGRESQL | MYSQL | ORACLE — auto-detected from jdbcUrl if omitted. */
    private String jdbcDialect;

    // ── FILE sink (010-B) ─────────────────────────────────────────────────────
    /** CSV | JSON | PARQUET */
    private String fileFormat = "CSV";
    /** Storage URI: file:///path, abfs://..., s3://... */
    private String storagePath;
    /** Roll output file on each Flink checkpoint. */
    private boolean rollOnCheckpoint = true;
    /** Max file size in bytes before rolling; 0 = unlimited (checkpoint-only rolling). */
    private long maxFileSizeBytes = 0;
    /** Optional partition field name — output/{partitionValue}/part-{taskId}. */
    private String partitionBy;
    private StorageConfig storage;

    // ── API sink (010-C) ──────────────────────────────────────────────────────
    private String url;
    private String method = "POST";
    /** Records per HTTP request; 1 = per-record, >1 = JSON array batch. */
    private int apiBatchSize = 1;
    private int connectTimeoutMs = 5000;
    private int readTimeoutMs = 10000;
    private int retryAttempts = 3;
    private long retryBackoffMs = 500;
    private ApiAuthConfig apiAuth;
    /** DLQ for records that the API permanently rejects (4xx after retries). */
    private DlqConfig dlq;
}
