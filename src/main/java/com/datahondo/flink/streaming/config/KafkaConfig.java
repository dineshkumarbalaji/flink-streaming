package com.datahondo.flink.streaming.config;

import lombok.Data;

import java.util.Map;

@Data
public class KafkaConfig {
    private String topic;
    private String bootstrapServers;
    private String groupId;
    private String startingOffset;
    private Long startingOffsetTimestamp;
    /**
     * Flink-native startup mode: earliest-offset, latest-offset, group-offsets, timestamp.
     * Takes priority over startingOffset when set.
     */
    private String startupMode;
    private String format; // STRING, JSON, AVRO
    private AuthConfig authentication;
    private Map<String, String> properties;
}