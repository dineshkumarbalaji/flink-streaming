package com.datahondo.flink.streaming.config;

import lombok.Data;

@Data
public class SchemaConfig {

    /**
     * Schema type: JSON, AVRO, or REGISTRY
     */
    private String type;

    /**
     * Inline schema definition (JSON Schema or Avro Schema JSON string).
     * Used when type is JSON or AVRO.
     */
    private String definition;

    /**
     * Schema Registry URL.
     * Used when type is REGISTRY.
     */
    private String registryUrl;

    /**
     * Schema Registry subject name.
     * Used when type is REGISTRY.
     */
    private String subject;

    public boolean hasDefinition() {
        return definition != null && !definition.isEmpty();
    }

    public boolean isRegistry() {
        return "REGISTRY".equalsIgnoreCase(type);
    }
}
