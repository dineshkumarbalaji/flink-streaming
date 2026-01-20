package com.antheon.flink.streaming.web;

import com.antheon.flink.streaming.config.*;
import com.antheon.flink.streaming.job.StreamingJobOrchestrator;
import com.antheon.flink.streaming.web.model.JobRequest;
import com.antheon.flink.streaming.web.model.ValidationResponse;
import com.antheon.flink.streaming.web.service.KafkaValidatorService;
import com.antheon.flink.streaming.web.service.SqlValidatorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {
    
    private final StreamingJobOrchestrator orchestrator;
    private final KafkaValidatorService validatorService;
    private final SqlValidatorService sqlValidatorService;


    private final StreamingJobConfig systemConfig;

    @PostMapping("/validate")
    public ResponseEntity<ValidationResponse> validateJob(@RequestBody JobRequest request) {
        log.info("Validating job request: {}", request.getJobName());
        List<String> logs = new ArrayList<>();
        logs.add("Starting validation for job: " + request.getJobName());
        log.info("Watermark Config - Enabled: {}, Mode: {}", request.isEnableWatermark(), request.getWatermarkMode());

        try {
            StreamingJobConfig config = mapToConfig(request);
            
            // Validate Source
            logs.add("Validating Source Config...");
            try {
                validatorService.validateConnection(config.getSource().getKafka());
                logs.add("✅ Source Kafka Connection OK");
                logs.add("✅ Source Topic '" + config.getSource().getKafka().getTopic() + "' Accessible");
            } catch (Exception e) {
                logs.add("❌ Source Validation Failed: " + e.getMessage());
                return ResponseEntity.ok(new ValidationResponse(false, logs));
            }
            
            // Validate Target
            logs.add("Validating Target Config...");
            try {
                validatorService.validateConnection(config.getTarget().getKafka());
                logs.add("✅ Target Kafka Connection OK");
                logs.add("✅ Target Topic '" + config.getTarget().getKafka().getTopic() + "' Accessible");
            } catch (Exception e) {
                logs.add("❌ Target Validation Failed: " + e.getMessage());
                return ResponseEntity.ok(new ValidationResponse(false, logs));
            }
            
            // Validate SQL
            logs.add("Validating SQL Query...");
            try {
                sqlValidatorService.validateSql(request.getSqlQuery(), request.getSourceTableName(), request.getSourceSchema(),
                        request.isEnableWatermark(), request.getWatermarkMode());
                logs.add("✅ SQL Syntax OK");
            } catch (Exception e) {
                logs.add("❌ SQL Validation Failed: " + e.getMessage());
                return ResponseEntity.ok(new ValidationResponse(false, logs));
            }
            
            logs.add("✅ All checks passed. Ready to deploy.");
            return ResponseEntity.ok(new ValidationResponse(true, logs));
            
        } catch (Exception e) {
            log.error("Validation error", e);
            logs.add("❌ Unexpected Error: " + e.getMessage());
            return ResponseEntity.ok(new ValidationResponse(false, logs));
        }
    }
    
    @PostMapping("/submit")
    public ResponseEntity<String> submitJob(@RequestBody JobRequest request) {
        log.info("Received job submission request: {}", request.getJobName());
        try {
            StreamingJobConfig config = mapToConfig(request);
            orchestrator.submitJob(config);
            saveJobConfig(request);
            return ResponseEntity.ok("Job '" + request.getJobName() + "' submitted successfully.");
        } catch (Exception e) {
            log.error("Failed to submit job", e);
            return ResponseEntity.internalServerError().body("Failed to submit job: " + e.getMessage());
        }
    }

    private void saveJobConfig(JobRequest request) {
        try {
            // Map to structured config
            com.antheon.flink.streaming.web.model.SavedJobConfig savedConfig = com.antheon.flink.streaming.web.model.SavedJobConfig.builder()
                .jobName(request.getJobName())
                .parallelism(request.getParallelism())
                .checkpointInterval(request.getCheckpointInterval())
                .source(com.antheon.flink.streaming.web.model.SavedJobConfig.SourceSection.builder()
                    .sourceTopic(request.getSourceTopic())
                    .sourceBootstrapServers(request.getSourceBootstrapServers())
                    .sourceGroupId(request.getSourceGroupId())
                    .sourceAuthType(request.getSourceAuthType())
                    .sourceUsername(request.getSourceUsername())
                    .sourcePassword(request.getSourcePassword())
                    .sourceMechanism(request.getSourceMechanism())
                    .sourceStartingOffset(request.getSourceStartingOffset())
                    .sourceStartingOffsetTimestamp(request.getSourceStartingOffsetTimestamp())
                    .sourceTableName(request.getSourceTableName())
                    .sourceSchema(request.getSourceSchema())
                    .enableWatermark(request.isEnableWatermark())
                    .watermarkMode(request.getWatermarkMode())
                    .watermarkColumn(request.getWatermarkColumn())
                    .sourceFormat(request.getSourceFormat())
                    .build())
                .transformation(com.antheon.flink.streaming.web.model.SavedJobConfig.TransformationSection.builder()
                    .sqlQuery(request.getSqlQuery())
                    .resultTableName(request.getResultTableName())
                    .build())
                .target(com.antheon.flink.streaming.web.model.SavedJobConfig.TargetSection.builder()
                    .targetTopic(request.getTargetTopic())
                    .targetBootstrapServers(request.getTargetBootstrapServers())
                    .targetAuthType(request.getTargetAuthType())
                    .targetUsername(request.getTargetUsername())
                    .targetPassword(request.getTargetPassword())
                    .targetMechanism(request.getTargetMechanism())
                    .targetFormat(request.getTargetFormat())
                    .targetSchema(request.getTargetSchema())
                    .build())
                .build();

            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
            String json = mapper.writeValueAsString(savedConfig);
            
            log.info("Job Configuration JSON:\n{}", json);
            
            String fileName = "configs/" + request.getJobName() + ".json";
            java.io.File file = new java.io.File(fileName);
            file.getParentFile().mkdirs();
            
            try (java.io.FileWriter writer = new java.io.FileWriter(file)) {
                writer.write(json);
                log.info("Saved job configuration to: {}", file.getAbsolutePath());
            }
        } catch (Exception e) {
            log.error("Failed to save job configuration", e);
        }
    }
    
    private StreamingJobConfig mapToConfig(JobRequest request) {
        StreamingJobConfig config = new StreamingJobConfig();
        config.setJobName(request.getJobName());
        
        // Source
        SourceConfig source = new SourceConfig();
        String sourceTableName = (request.getSourceTableName() != null && !request.getSourceTableName().isEmpty()) 
                ? request.getSourceTableName() : "source_table";
        source.setTableName(sourceTableName);
        source.setSchema(request.getSourceSchema());
        
        KafkaConfig sourceKafka = new KafkaConfig();
        sourceKafka.setTopic(request.getSourceTopic());
        sourceKafka.setBootstrapServers(request.getSourceBootstrapServers());
        sourceKafka.setGroupId(request.getSourceGroupId());
        
        if (request.getSourceAuthType() != null && !request.getSourceAuthType().equals("NONE")) {
            AuthConfig auth = new AuthConfig();
            auth.setType(request.getSourceAuthType());
            auth.setUsername(request.getSourceUsername());
            auth.setPassword(request.getSourcePassword());
            auth.setMechanism(request.getSourceMechanism());
            sourceKafka.setAuthentication(auth);
        }
        

        
        sourceKafka.setStartingOffset(request.getSourceStartingOffset());
        sourceKafka.setStartingOffsetTimestamp(request.getSourceStartingOffsetTimestamp());
        sourceKafka.setFormat(request.getSourceFormat()); // Map format
        
        source.setKafka(sourceKafka);
        
        // Watermark
        WatermarkConfig watermark = new WatermarkConfig();
        if (request.isEnableWatermark()) {
            watermark.setStrategy("BOUNDED");
            watermark.setMode(request.getWatermarkMode());
            watermark.setTimestampColumn(request.getWatermarkColumn());
            watermark.setMaxOutOfOrderness(5000); // Default 5s
        } else {
            watermark.setStrategy("NONE");
        }
        source.setWatermark(watermark);
        
        config.setSource(source);
        
        // Transformation
        TransformationConfig transformation = new TransformationConfig();
        transformation.setSqlContent(request.getSqlQuery());
        String resultTableName = (request.getResultTableName() != null && !request.getResultTableName().isEmpty()) 
                ? request.getResultTableName() : "result_table";
        transformation.setResultTableName(resultTableName);
        config.setTransformation(transformation);
        
        // Target
        TargetConfig target = new TargetConfig();
        KafkaConfig targetKafka = new KafkaConfig();
        targetKafka.setTopic(request.getTargetTopic());
        targetKafka.setBootstrapServers(request.getTargetBootstrapServers());
        
        if (request.getTargetAuthType() != null && !request.getTargetAuthType().equals("NONE")) {
            AuthConfig auth = new AuthConfig();
            auth.setType(request.getTargetAuthType());
            auth.setUsername(request.getTargetUsername());
            auth.setPassword(request.getTargetPassword());
            auth.setMechanism(request.getTargetMechanism());
            targetKafka.setAuthentication(auth);
        }
        
        
        targetKafka.setFormat(request.getTargetFormat());
        target.setKafka(targetKafka);
        target.setSchema(request.getTargetSchema()); // Set target schema
        config.setTarget(target);
        
        // Flink
        FlinkConfig flink = new FlinkConfig();
        
        // Copy infrastructure settings from system config
        if (systemConfig.getFlink() != null) {
            log.info("System Config Flink: Remote={}, Host={}, Port={}, JarPath={}", 
                    systemConfig.getFlink().isRemote(),
                    systemConfig.getFlink().getHost(),
                    systemConfig.getFlink().getPort(),
                    systemConfig.getFlink().getJarPath());

            flink.setRemote(systemConfig.getFlink().isRemote());
            flink.setHost(systemConfig.getFlink().getHost());
            flink.setPort(systemConfig.getFlink().getPort());
            
            String jarPath = systemConfig.getFlink().getJarPath();
            // Fallback to default Docker path if null
            if (jarPath == null || jarPath.isEmpty()) {
                log.warn("JarPath is missing in configuration. Defaulting to /app/flink-job.jar");
                jarPath = "/app/flink-job.jar";
            }
            flink.setJarPath(jarPath);
        } else {
             // Fallback if flink config is completely missing
             log.warn("System Flink Config is NULL. Using defaults.");
             flink.setRemote(true); // Default to remote in Docker?
             flink.setHost("jobmanager");
             flink.setPort(8081);
             flink.setJarPath("/app/flink-job.jar");
        }
        
        flink.setParallelism(request.getParallelism() != null ? request.getParallelism() : 1);
        flink.setCheckpointInterval(request.getCheckpointInterval() != null ? request.getCheckpointInterval() : 60000L);
        config.setFlink(flink);
        
        return config;
    }
}
