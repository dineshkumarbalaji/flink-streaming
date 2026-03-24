package com.datahondo.flink.streaming.job;

import com.datahondo.flink.streaming.config.StreamingJobConfig;
import com.datahondo.flink.streaming.source.KafkaSourceLayer;
import com.datahondo.flink.streaming.target.KafkaTargetLayer;
import com.datahondo.flink.streaming.transformation.TransformationLayer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class StreamingJobOrchestrator {

    // Removed injected config
    private final KafkaSourceLayer sourceLayer;
    private final TransformationLayer transformationLayer;
    private final KafkaTargetLayer targetLayer;

    private final Map<String, JobClient> runningJobs = new LinkedHashMap<>();

    public List<Map<String, String>> getRunningJobs() {
        List<Map<String, String>> result = new ArrayList<>();
        runningJobs.forEach((name, client) -> {
            Map<String, String> info = new LinkedHashMap<>();
            info.put("jobName", name);
            info.put("jobId", client.getJobID().toString());
            try {
                info.put("status", client.getJobStatus().get().name());
            } catch (Exception e) {
                info.put("status", "UNKNOWN");
            }
            result.add(info);
        });
        return result;
    }

    public JobClient submitJob(StreamingJobConfig config) throws Exception {
        log.info("Starting Flink Streaming Job: {}", config.getJobName());
        
        // Create Flink environment
        StreamExecutionEnvironment env;
        if (config.getFlink().isRemote()) {
            log.info("Creating REMOTE Flink environment: {}:{}", config.getFlink().getHost(), config.getFlink().getPort());
            env = StreamExecutionEnvironment.createRemoteEnvironment(
                config.getFlink().getHost(),
                config.getFlink().getPort(),
                config.getFlink().getJarPath()
            );
        } else {
            log.info("Creating LOCAL Flink environment");
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Configure Flink environment
        configureFlinkEnvironment(env, config);
        
        // Layer 1: Source - Read from Kafka
        log.info("=== Layer 1: Source Layer ===");
        // Layer 1: Source - Read from Kafka
        log.info("=== Layer 1: Source Layer ===");
        if (config.getSources() != null) {
            for (com.datahondo.flink.streaming.config.SourceConfig sourceConfig : config.getSources()) {
                sourceLayer.createSourceTable(env, tableEnv, sourceConfig);
            }
        }
        
        // Layer 2: Transformation - Apply business logic
        log.info("=== Layer 2: Transformation Layer ===");
        Table transformedTable = transformationLayer.applyTransformation(
                tableEnv, 
                config.getTransformation()
        );
        
        // Layer 3: Target - Sink to Kafka
        log.info("=== Layer 3: Target Layer ===");
        targetLayer.sinkToKafka(tableEnv, transformedTable, config.getTarget());
        
        // Execute the job asynchronously
        log.info("Submitting Flink job async...");
        // env.executeAsync() returns a JobClient
        JobClient jobClient = env.executeAsync(config.getJobName());
        runningJobs.put(config.getJobName(), jobClient);
        return jobClient;
    }
    
    private void configureFlinkEnvironment(StreamExecutionEnvironment env, StreamingJobConfig config) {
        // Set parallelism
        if (config.getFlink().getParallelism() != null) {
            env.setParallelism(config.getFlink().getParallelism());
        }
        
        // Enable checkpointing
        if (config.getFlink().getCheckpointInterval() != null) {
            env.enableCheckpointing(config.getFlink().getCheckpointInterval());
        }
        
        if (config.getFlink().getMaxConcurrentCheckpoints() != null) {
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(
                    config.getFlink().getMaxConcurrentCheckpoints()
            );
        }
        
        // Set state backend
        if (config.getFlink().getCheckpointDir() != null) {
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(config.getFlink().getCheckpointDir());
        }
        
        log.info("Flink environment configured - Parallelism: {}, Checkpoint Interval: {}ms",
                config.getFlink().getParallelism(),
                config.getFlink().getCheckpointInterval());
    }
}