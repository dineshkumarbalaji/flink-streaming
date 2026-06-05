package com.datahondo.flink.streaming.config;

import lombok.Data;

/**
 * Per-source configuration. The {@code type} field selects the source layer:
 * KAFKA (default) | FILE | JDBC | API
 */
@Data
public class SourceConfig {

    /** Source type discriminator. Defaults to KAFKA for backward compatibility. */
    private String type = "KAFKA";

    // ── Common ────────────────────────────────────────────────────────────────
    private KafkaConfig kafka;
    private String tableName;
    /**
     * Optional SQL alias used in multi-source JOIN queries.
     * e.g. alias="s1" → SELECT * FROM source_table s1 JOIN source_table2 s2 ON s1.id = s2.id
     */
    private String alias;
    private SchemaConfig schema;
    private WatermarkConfig watermark;
    /** Per-source DLQ config. When set and enabled, rejected records are routed to a DLQ topic. */
    private DlqConfig dlq;

    // ── FILE source (009-A) ───────────────────────────────────────────────────
    /** CSV | JSON | PARQUET */
    private String fileFormat = "CSV";
    /** Storage URI: file:///path, abfs://container@account.dfs.core.windows.net/path, s3://bucket/prefix */
    private String storagePath;
    /** Scan subdirectories recursively. */
    private boolean recursive = false;
    /** 0 = one-shot batch read; >0 = continuous file monitoring interval in ms. */
    private long monitorInterval = 0;
    /** Cloud storage credentials (ADLS / S3). */
    private StorageConfig storage;

    // ── JDBC source (009-B) ───────────────────────────────────────────────────
    private String jdbcUrl;
    /** Full SELECT query to execute. */
    private String query;
    private int fetchSize = 1000;
    /** Number of parallel partitions; requires partitionColumn + lowerBound + upperBound. */
    private int numPartitions = 1;
    private String partitionColumn;
    private Long lowerBound;
    private Long upperBound;
    /** JDBC auth; reuses AuthConfig username/password pattern via ApiAuthConfig BEARER type. */
    private String jdbcUsername;
    private String jdbcPassword;
    /** disable | require | verify-full */
    private String sslMode;
    private String sslCertPath;

    // ── API source (009-C) ────────────────────────────────────────────────────
    /** REST endpoint URL. */
    private String url;
    /** HTTP method for polling — GET or POST. */
    private String method = "GET";
    /** Polling interval in ms. */
    private long pollIntervalMs = 5000;
    /** JSONPath expression to extract the records array from the response body. */
    private String jsonPath;
    private int connectTimeoutMs = 5000;
    private int readTimeoutMs = 10000;
    private int retryAttempts = 3;
    private long retryBackoffMs = 500;
    /** Authentication for API source. */
    private ApiAuthConfig apiAuth;
}
