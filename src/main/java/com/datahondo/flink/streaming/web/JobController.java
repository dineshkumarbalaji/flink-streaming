package com.datahondo.flink.streaming.web;

import com.datahondo.flink.streaming.config.*;
import com.datahondo.flink.streaming.job.StreamingJobOrchestrator;
import com.datahondo.flink.streaming.web.model.JobRequest;
import com.datahondo.flink.streaming.web.model.ValidationResponse;
import com.datahondo.flink.streaming.web.service.KafkaValidatorService;
import com.datahondo.flink.streaming.web.service.SqlValidatorService;
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
    
    private static final String DEFAULT_FLINK_HOST = "jobmanager";
    private static final int DEFAULT_FLINK_PORT = 8081;
    private static final String DEFAULT_JAR_PATH = "/app/flink-job.jar";

    @GetMapping("/list")
    public ResponseEntity<java.util.List<java.util.Map<String, String>>> listJobs() {
        return ResponseEntity.ok(orchestrator.getRunningJobs());
    }

    @PostMapping("/validate")
    public ResponseEntity<ValidationResponse> validateJob(@RequestBody JobRequest request) {
        log.info("Validating job request: {}", request.getJobName());
        List<String> logs = new ArrayList<>();
        logs.add("Starting validation for job: " + request.getJobName());
        if (request.getSources() != null && !request.getSources().isEmpty()) {
            JobRequest.SourceJobRequest firstSrc = request.getSources().get(0);
            log.info("Watermark Config - Enabled: {}, Mode: {}", firstSrc.isEnableWatermark(), firstSrc.getWatermarkMode());
        }

        try {
            StreamingJobConfig config = mapToConfig(request);
            
            // Validate Sources
            logs.add("Validating Source Configs...");
            if (config.getSources() == null || config.getSources().isEmpty()) {
                logs.add("❌ No sources configured");
                return ResponseEntity.ok(new ValidationResponse(false, logs));
            }

            for (SourceConfig source : config.getSources()) {
                try {
                    validatorService.validateConnection(source.getKafka());
                    logs.add("✅ Source '" + source.getKafka().getTopic() + "' Connection OK");
                } catch (Exception e) {
                    logs.add("❌ Source '" + source.getKafka().getTopic() + "' Validation Failed: " + e.getMessage());
                    return ResponseEntity.ok(new ValidationResponse(false, logs));
                }
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
                // Use the first source's table/schema for SQL validation for now, 
                // or just pass main requirements. Ideally validator should know about all tables.
                // For now passing first source details or empty if multi-source complex logic
                String firstSourceTable = config.getSources().get(0).getTableName();
                String firstSourceSchema = config.getSources().get(0).getSchema();
                
                JobRequest.SourceJobRequest firstSrc = request.getSources().get(0);
                String watermarkMode = firstSrc.getWatermarkMode();
                boolean hasWatermark = firstSrc.isEnableWatermark() && watermarkMode != null && !watermarkMode.equals("NONE");
                sqlValidatorService.validateSql(request.getSqlQuery(), firstSourceTable, firstSourceSchema,
                        hasWatermark, watermarkMode);
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
            com.datahondo.flink.streaming.web.model.SavedJobConfig savedConfig = com.datahondo.flink.streaming.web.model.SavedJobConfig.builder()
                .jobName(request.getJobName())
                .parallelism(request.getParallelism())
                .checkpointInterval(request.getCheckpointInterval())
                .sources(mapSourcesToSavedConfig(request.getSources()))
                .transformation(com.datahondo.flink.streaming.web.model.SavedJobConfig.TransformationSection.builder()
                    .sqlQuery(request.getSqlQuery())
                    .resultTableName(request.getResultTableName())
                    .build())
                .target(com.datahondo.flink.streaming.web.model.SavedJobConfig.TargetSection.builder()
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
        // Sources
        List<SourceConfig> sources = new ArrayList<>();
        if (request.getSources() != null) {
            for (JobRequest.SourceJobRequest srcReq : request.getSources()) {
                SourceConfig source = new SourceConfig();
                String sourceTableName = (srcReq.getSourceTableName() != null && !srcReq.getSourceTableName().isEmpty()) 
                        ? srcReq.getSourceTableName() : "source_table_" + sources.size();
                source.setTableName(sourceTableName);
                source.setSchema(srcReq.getSourceSchema());
                
                KafkaConfig sourceKafka = new KafkaConfig();
                sourceKafka.setTopic(srcReq.getSourceTopic());
                sourceKafka.setBootstrapServers(srcReq.getSourceBootstrapServers());
                sourceKafka.setGroupId(srcReq.getSourceGroupId());
                
                if (srcReq.getSourceAuthType() != null && !srcReq.getSourceAuthType().equals("NONE")) {
                    AuthConfig auth = new AuthConfig();
                    auth.setType(srcReq.getSourceAuthType());
                    auth.setUsername(srcReq.getSourceUsername());
                    auth.setPassword(srcReq.getSourcePassword());
                    auth.setMechanism(srcReq.getSourceMechanism());
                    sourceKafka.setAuthentication(auth);
                }
                
                sourceKafka.setStartingOffset(srcReq.getSourceStartingOffset());
                sourceKafka.setStartingOffsetTimestamp(srcReq.getSourceStartingOffsetTimestamp());
                sourceKafka.setFormat(srcReq.getSourceFormat());
                
                source.setKafka(sourceKafka);
                
                // Watermark
                WatermarkConfig watermark = new WatermarkConfig();
                if (srcReq.isEnableWatermark()) {
                    watermark.setStrategy("BOUNDED");
                    watermark.setMode(srcReq.getWatermarkMode());
                    watermark.setTimestampColumn(srcReq.getWatermarkColumn());
                    watermark.setMaxOutOfOrderness(5000);
                } else {
                    watermark.setStrategy("NONE");
                }
                source.setWatermark(watermark);
                
                sources.add(source);
            }
        }
        config.setSources(sources);
        
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
                 log.warn("JarPath is missing in configuration. Defaulting to {}", DEFAULT_JAR_PATH);
                jarPath = DEFAULT_JAR_PATH;
            }
            flink.setJarPath(jarPath);
        } else {
             // Fallback if flink config is completely missing
             log.warn("System Flink Config is NULL. Using defaults.");
             flink.setRemote(true); // Default to remote in Docker?
             flink.setHost(DEFAULT_FLINK_HOST);
             flink.setPort(DEFAULT_FLINK_PORT);
             flink.setJarPath(DEFAULT_JAR_PATH);
        }
        
        flink.setParallelism(request.getParallelism() != null ? request.getParallelism() : 1);
        flink.setCheckpointInterval(request.getCheckpointInterval() != null ? request.getCheckpointInterval() : 60000L);
        config.setFlink(flink);
        
        return config;
    }

    private List<com.datahondo.flink.streaming.web.model.SavedJobConfig.SourceSection> mapSourcesToSavedConfig(List<JobRequest.SourceJobRequest> sourceRequests) {
        List<com.datahondo.flink.streaming.web.model.SavedJobConfig.SourceSection> savedSources = new ArrayList<>();
        if (sourceRequests != null) {
            for (JobRequest.SourceJobRequest req : sourceRequests) {
                savedSources.add(com.datahondo.flink.streaming.web.model.SavedJobConfig.SourceSection.builder()
                        .sourceTopic(req.getSourceTopic())
                        .sourceBootstrapServers(req.getSourceBootstrapServers())
                        .sourceGroupId(req.getSourceGroupId())
                        .sourceAuthType(req.getSourceAuthType())
                        .sourceUsername(req.getSourceUsername())
                        .sourcePassword(req.getSourcePassword())
                        .sourceMechanism(req.getSourceMechanism())
                        .sourceStartingOffset(req.getSourceStartingOffset())
                        .sourceStartingOffsetTimestamp(req.getSourceStartingOffsetTimestamp())
                        .sourceTableName(req.getSourceTableName())
                        .sourceSchema(req.getSourceSchema())
                        .enableWatermark(req.isEnableWatermark())
                        .watermarkMode(req.getWatermarkMode())
                        .watermarkColumn(req.getWatermarkColumn())
                        .sourceFormat(req.getSourceFormat())
                        .build());
            }
        }
        return savedSources;
    }
}
