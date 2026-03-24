package com.datahondo.flink.streaming.config;

import lombok.Data;

@Data
public class SourceConfig {
    private KafkaConfig kafka;
    private String tableName;
    private String schema;
    private WatermarkConfig watermark;
}