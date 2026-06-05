package com.datahondo.flink.streaming.config;

import lombok.Data;
import java.util.List;

@Data
public class SchemaConfig {

    /** Schema type: JSON | AVRO | REGISTRY */
    private String type;

    /**
     * Inline schema definition (JSON Schema or Avro Schema JSON string).
     * Used when type is JSON or AVRO.
     */
    private String definition;

    /** Typed field list used by JDBC and File source/sink for column mapping. */
    private List<SchemaField> fields;

    // ── Schema Registry (Feature 011) ────────────────────────────────────────

    /** Schema Registry base URL. Used when type is REGISTRY. */
    private String registryUrl;

    /** Subject name; defaults to <topic>-value if omitted. */
    private String subject;

    /** Schema version to fetch — a numeric version or "latest". */
    private String version = "latest";

    /** TTL for the local schema cache in ms. Default 5 minutes. */
    private long cacheTtlMs = 300_000L;

    /** SASL mechanism: PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 */
    private String saslMechanism = "PLAIN";

    private String registryUsername;
    private String registryPassword;

    /** TLS configuration for the registry connection. */
    private RegistryTlsConfig tls;

    @Data
    public static class RegistryTlsConfig {
        private boolean enabled = false;
        private String truststorePath;
        private String truststorePassword;
        private boolean skipHostnameVerification = false;
    }

    /**
     * Typed field descriptor used by JDBC source/sink, File source/sink,
     * and SQL validator for column-level metadata.
     */
    @Data
    public static class SchemaField {
        private String name;
        /** Flink SQL type: STRING, INT, BIGINT, DOUBLE, BOOLEAN, TIMESTAMP, etc. */
        private String type;
        private boolean nullable = true;
        /** Marks this field as part of the upsert primary key (JDBC sink). */
        private boolean primaryKey = false;
    }

    public boolean hasDefinition() {
        return definition != null && !definition.isEmpty();
    }

    public boolean isRegistry() {
        return "REGISTRY".equalsIgnoreCase(type);
    }

    public boolean hasFields() {
        return fields != null && !fields.isEmpty();
    }
}
