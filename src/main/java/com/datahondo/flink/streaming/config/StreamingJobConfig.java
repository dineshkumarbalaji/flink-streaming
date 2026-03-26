package com.datahondo.flink.streaming.config;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "streaming.job")
public class StreamingJobConfig {

    private String jobName;
    private java.util.List<SourceConfig> sources;
    private TransformationConfig transformation;
    private TargetConfig target;
    private FlinkConfig flink;

    /** Optional audit configuration. When null, audit is disabled. */
    private AuditConfig audit;

    /** Optional reconciliation configuration. When null, reconciliation is disabled. */
    private ReconciliationConfig reconciliation;
}
