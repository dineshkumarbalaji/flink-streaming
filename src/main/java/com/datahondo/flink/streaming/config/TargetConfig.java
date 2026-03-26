package com.datahondo.flink.streaming.config;

import lombok.Data;

@Data
public class TargetConfig {
    /**
     * Sink type: KAFKA, JDBC, ICEBERG, FILE.
     * Defaults to KAFKA if not set.
     */
    private String type = "KAFKA";
    private KafkaConfig kafka;
    private SchemaConfig schema;
}
