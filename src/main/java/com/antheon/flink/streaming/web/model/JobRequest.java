package com.antheon.flink.streaming.web.model;

import lombok.Data;

@Data
public class JobRequest {
    private String jobName;
    
    // Source
    private String sourceTopic;
    private String sourceBootstrapServers;
    private String sourceGroupId;
    private String sourceAuthType; // NONE, SASL_SSL, SASL_PLAINTEXT
    private String sourceUsername;
    private String sourcePassword;
    private String sourceMechanism; // PLAIN, SCRAM-SHA-256
    private String sourceStartingOffset;
    private Long sourceStartingOffsetTimestamp;
    private String sourceTableName;
    private String sourceSchema;
    private String sourceFormat; // STRING, JSON, AVRO
    
    // Watermark
    private boolean enableWatermark;
    private String watermarkMode; // PROCESS_TIME, EXISTING
    private String watermarkColumn;
    
    // Transformation
    private String sqlQuery;
    private String resultTableName;
    
    // Target
    private String targetTopic;
    private String targetBootstrapServers;
    private String targetAuthType;
    private String targetUsername;
    private String targetPassword;
    private String targetMechanism;
    private String targetFormat; // STRING, JSON, AVRO
    private String targetSchema;
    
    // Flink
    private Integer parallelism;
    private Long checkpointInterval;
}
