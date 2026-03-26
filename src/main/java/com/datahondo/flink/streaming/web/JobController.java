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
    private static final String REDACTED = "***REDACTED***";

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
                SchemaConfig firstSourceSchemaConfig = config.getSources().get(0).getSchema();
                String firstSourceSchema = (firstSourceSchemaConfig != null) ? firstSourceSchemaConfig.getDefinition() : null;

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
            
            // Validate Checkpoint Directory URI (if provided)
            logs.add("Validating Flink Config...");
            String checkpointDir = request.getCheckpointDir();
            if (checkpointDir != null && !checkpointDir.isEmpty()) {
                if (checkpointDir.matches("(?i)file://[^/].*")) {
                    logs.add("❌ Invalid checkpoint directory '" + checkpointDir
                            + "': local paths require three slashes, e.g. file:///tmp/checkpoints");
                    return ResponseEntity.ok(new ValidationResponse(false, logs));
                }
                logs.add("✅ Checkpoint directory URI format OK");
            } else {
                logs.add("✅ Checkpoint directory: using cluster default");
            }

            logs.add("✅ All checks passed. Ready to deploy.");
            return ResponseEntity.ok(new ValidationResponse(true, logs));
            
        } catch (Exception e) {
            log.error("Validation error", e);
            logs.add("❌ Unexpected Error: " + e.getMessage());
            return ResponseEntity.ok(new ValidationResponse(false, logs));
        }
    }
    
    @DeleteMapping("/{jobName}")
    public ResponseEntity<String> cancelJob(@PathVariable String jobName) {
        log.info("Received cancel request for job: {}", jobName);
        try {
            orchestrator.cancelJob(jobName);
            return ResponseEntity.ok("Job '" + jobName + "' cancelled.");
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            log.error("Failed to cancel job '{}'", jobName, e);
            return ResponseEntity.internalServerError().body("Failed to cancel job: " + e.getMessage());
        }
    }

    @PostMapping("/submit")
    public ResponseEntity<String> submitJob(@RequestBody JobRequest request) {
        log.info("Received job submission request: {}", request.getJobName());
        if (request.getSources() == null || request.getSources().isEmpty()) {
            return ResponseEntity.badRequest().body("Job must have at least one source configured.");
        }
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
                    .targetSchemaType(request.getTargetSchemaType())
                    .targetSchemaRegistryUrl(request.getTargetSchemaRegistryUrl())
                    .targetSchemaSubject(request.getTargetSchemaSubject())
                    .build())
                .build();

            redactPasswords(savedConfig);

            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
            String json = mapper.writeValueAsString(savedConfig);

            log.info("Saving job configuration for '{}' ({} source(s))",
                    savedConfig.getJobName(),
                    savedConfig.getSources() != null ? savedConfig.getSources().size() : 0);
            
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
                source.setAlias(srcReq.getSourceAlias());
                source.setSchema(buildSchemaConfig(srcReq.getSourceSchema(), srcReq.getSourceSchemaType(),
                        srcReq.getSourceSchemaRegistryUrl(), srcReq.getSourceSchemaSubject()));

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
                    auth.setTruststoreLocation(srcReq.getSourceTruststoreLocation());
                    auth.setTruststorePassword(srcReq.getSourceTruststorePassword());
                    auth.setJaasConfig(srcReq.getSourceJaasConfig());
                    sourceKafka.setAuthentication(auth);
                }

                sourceKafka.setStartupMode(srcReq.getSourceStartupMode());
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
                    long maxOutOfOrderness = srcReq.getWatermarkMaxOutOfOrderness() != null
                            ? srcReq.getWatermarkMaxOutOfOrderness() : 5000L;
                    watermark.setMaxOutOfOrderness(maxOutOfOrderness);
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
        transformation.setType(request.getTransformationType());
        transformation.setSqlContent(request.getSqlQuery());
        transformation.setSqlFilePath(request.getSqlFilePath());
        String resultTableName = (request.getResultTableName() != null && !request.getResultTableName().isEmpty())
                ? request.getResultTableName() : "result_table";
        transformation.setResultTableName(resultTableName);
        config.setTransformation(transformation);

        // Target
        TargetConfig target = new TargetConfig();
        if (request.getTargetType() != null) target.setType(request.getTargetType());
        KafkaConfig targetKafka = new KafkaConfig();
        targetKafka.setTopic(request.getTargetTopic());
        targetKafka.setBootstrapServers(request.getTargetBootstrapServers());

        if (request.getTargetAuthType() != null && !request.getTargetAuthType().equals("NONE")) {
            AuthConfig auth = new AuthConfig();
            auth.setType(request.getTargetAuthType());
            auth.setUsername(request.getTargetUsername());
            auth.setPassword(request.getTargetPassword());
            auth.setMechanism(request.getTargetMechanism());
            auth.setTruststoreLocation(request.getTargetTruststoreLocation());
            auth.setTruststorePassword(request.getTargetTruststorePassword());
            auth.setJaasConfig(request.getTargetJaasConfig());
            targetKafka.setAuthentication(auth);
        }
        
        
        targetKafka.setFormat(request.getTargetFormat());
        target.setKafka(targetKafka);
        target.setSchema(buildSchemaConfig(request.getTargetSchema(), request.getTargetSchemaType(),
                request.getTargetSchemaRegistryUrl(), request.getTargetSchemaSubject()));
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
        if (request.getCheckpointDir() != null && !request.getCheckpointDir().isEmpty()) {
            flink.setCheckpointDir(request.getCheckpointDir());
        } else if (systemConfig.getFlink() != null) {
            flink.setCheckpointDir(systemConfig.getFlink().getCheckpointDir());
        }
        config.setFlink(flink);

        // Audit and reconciliation come from application.yml (system config), not per-job request
        config.setAudit(systemConfig.getAudit());
        if (systemConfig.getReconciliation() != null) {
            ReconciliationConfig recon = systemConfig.getReconciliation();
            recon.setWindow(ReconciliationConfig.windowFromCheckpointInterval(flink.getCheckpointInterval()));
            config.setReconciliation(recon);
        }

        return config;
    }

    private SchemaConfig buildSchemaConfig(String definition, String type, String registryUrl, String subject) {
        if ((definition == null || definition.isEmpty()) && type == null) {
            return null;
        }
        SchemaConfig schema = new SchemaConfig();
        schema.setDefinition(definition);
        schema.setType(type);
        schema.setRegistryUrl(registryUrl);
        schema.setSubject(subject);
        return schema;
    }

    private void redactPasswords(com.datahondo.flink.streaming.web.model.SavedJobConfig config) {
        if (config.getSources() != null) {
            for (com.datahondo.flink.streaming.web.model.SavedJobConfig.SourceSection src : config.getSources()) {
                if (src.getSourcePassword() != null) src.setSourcePassword(REDACTED);
            }
        }
        if (config.getTarget() != null && config.getTarget().getTargetPassword() != null) {
            config.getTarget().setTargetPassword(REDACTED);
        }
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
                        .sourceSchemaType(req.getSourceSchemaType())
                        .sourceSchemaRegistryUrl(req.getSourceSchemaRegistryUrl())
                        .sourceSchemaSubject(req.getSourceSchemaSubject())
                        .enableWatermark(req.isEnableWatermark())
                        .watermarkMode(req.getWatermarkMode())
                        .watermarkColumn(req.getWatermarkColumn())
                        .watermarkMaxOutOfOrderness(req.getWatermarkMaxOutOfOrderness())
                        .sourceFormat(req.getSourceFormat())
                        .build());
            }
        }
        return savedSources;
    }
}
