package com.antheon.flink.streaming.config;

import lombok.Data;

import java.util.Map;

@Data
public class KafkaConfig {
    private String topic;
    private String bootstrapServers;
    private String groupId;
    private String startingOffset;
    private Long startingOffsetTimestamp;
    private String format; // STRING, JSON, AVRO
    private AuthConfig authentication;
    private Map<String, String> properties;
}