package com.datahondo.flink.streaming.web.model;

import java.util.List;

import lombok.Data;

@Data
public class JobRequest {
    private String jobName;
    
    // Source
    private List<SourceJobRequest> sources;
    
    // Transformation
    private String sqlQuery;
    private String resultTableName;
    
    // Transformation
    private String transformationType; // INLINE, FILE
    private String sqlFilePath;

    // Target
    private String targetType; // KAFKA, JDBC, ICEBERG, FILE
    private String targetTopic;
    private String targetBootstrapServers;
    private String targetAuthType;
    private String targetUsername;
    private String targetPassword;
    private String targetMechanism;
    private String targetTruststoreLocation;
    private String targetTruststorePassword;
    private String targetJaasConfig;
    private String targetFormat; // STRING, JSON, AVRO
    private String targetSchema;
    private String targetSchemaType;       // JSON, AVRO, REGISTRY
    private String targetSchemaRegistryUrl;
    private String targetSchemaSubject;
    
    // Flink
    private Integer parallelism;
    private Long checkpointInterval;
    private String checkpointDir;

    @Data
    public static class SourceJobRequest {
        private String sourceTopic;
        private String sourceBootstrapServers;
        private String sourceGroupId;
        private String sourceAuthType; // NONE, SASL_SSL, SASL_PLAINTEXT
        private String sourceUsername;
        private String sourcePassword;
        private String sourceMechanism; // PLAIN, SCRAM-SHA-256
        private String sourceStartingOffset;
        private String sourceStartupMode;      // earliest-offset, latest-offset, group-offsets, timestamp
        private Long sourceStartingOffsetTimestamp;
        private String sourceTableName;
        private String sourceAlias;            // SQL alias for JOIN queries
        private String sourceSchema;
        private String sourceSchemaType;       // JSON, AVRO, REGISTRY
        private String sourceSchemaRegistryUrl;
        private String sourceSchemaSubject;
        private String sourceTruststoreLocation;
        private String sourceTruststorePassword;
        private String sourceJaasConfig;
        private String sourceFormat; // STRING, JSON, AVRO

        // Watermark
        private boolean enableWatermark;
        private String watermarkMode; // PROCESS_TIME, EXISTING
        private String watermarkColumn;
        private Long watermarkMaxOutOfOrderness; // ms, used with EXISTING mode
    }
}
