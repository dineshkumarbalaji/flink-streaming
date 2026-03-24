package com.datahondo.flink.streaming.web.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SavedJobConfig {
    private String jobName;
    private Integer parallelism;
    private Long checkpointInterval;
    
    private List<SourceSection> sources;
    private TransformationSection transformation;
    private TargetSection target;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SourceSection {
        private String sourceTopic;
        private String sourceBootstrapServers;
        private String sourceGroupId;
        private String sourceAuthType;
        private String sourceUsername;
        private String sourcePassword;
        private String sourceMechanism;
        private String sourceStartingOffset;
        private Long sourceStartingOffsetTimestamp;
        private String sourceTableName;
        private String sourceSchema;
        private String sourceFormat; // STRING, JSON, AVRO
        
        private boolean enableWatermark;
        private String watermarkMode;
        private String watermarkColumn;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TransformationSection {
        private String sqlQuery;
        private String resultTableName;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TargetSection {
        private String targetTopic;
        private String targetBootstrapServers;
        private String targetAuthType;
        private String targetUsername;
        private String targetPassword;
        private String targetMechanism;
        private String targetFormat; // STRING, JSON, AVRO
        private String targetSchema;
    }
}
