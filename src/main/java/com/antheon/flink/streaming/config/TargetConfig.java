package com.antheon.flink.streaming.config;

import lombok.Data;

@Data
public class TargetConfig {
    private KafkaConfig kafka;
    private String schema;
}
