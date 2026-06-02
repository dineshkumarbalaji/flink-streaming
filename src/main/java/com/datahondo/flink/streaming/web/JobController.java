package com.datahondo.flink.streaming.web;

import com.datahondo.flink.streaming.config.*;
import com.datahondo.flink.streaming.job.StreamingJobOrchestrator;
import com.datahondo.flink.streaming.job.audit.JobAuditRecord;
import com.datahondo.flink.streaming.job.audit.JobAuditService;
import com.datahondo.flink.streaming.job.audit.JobStatusPoller;
import com.datahondo.flink.streaming.savepoint.SavepointException;
import com.datahondo.flink.streaming.savepoint.SavepointRecord;
import com.datahondo.flink.streaming.savepoint.SavepointRegistry;
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
    private final com.datahondo.flink.streaming.audit.InMemoryAuditCache auditCache;
    private final SavepointRegistry savepointRegistry;
    private final JobAuditService jobAuditService;
    private final JobStatusPoller jobStatusPoller;

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
            
            // Validate SQL — register all source tables so multi-source JOINs are validated.
            logs.add("Validating SQL Query...");
            try {
                List<SqlValidatorService.SourceEntry> sourceEntries = new ArrayList<>();
                List<SourceConfig> configSources = config.getSources();
                List<JobRequest.SourceJobRequest> reqSources = request.getSources();
                for (int i = 0; i < configSources.size(); i++) {
                    SourceConfig src = configSources.get(i);
                    JobRequest.SourceJobRequest srcReq = reqSources.get(i);
                    String schema = (src.getSchema() != null) ? src.getSchema().getDefinition() : null;
                    String watermarkMode = srcReq.getWatermarkMode();
                    boolean hasWatermark = srcReq.isEnableWatermark()
                            && watermarkMode != null && !watermarkMode.equals("NONE");
                    sourceEntries.add(new SqlValidatorService.SourceEntry(
                            src.getTableName(), schema, hasWatermark, watermarkMode));
                }
                sqlValidatorService.validateSql(request.getSqlQuery(), sourceEntries);
                logs.add("✅ SQL Syntax OK");
            } catch (Exception e) {
                logs.add("❌ SQL Validation Failed: " + e.getMessage());
                return ResponseEntity.ok(new ValidationResponse(false, logs));
            }
            
            // Validate Checkpoint Directory URI (if provided)
            logs.add("Validating Flink Config...");
            String checkpointDirError = validateCheckpointDir(request.getCheckpointDir(), logs);
            if (checkpointDirError != null) {
                return ResponseEntity.ok(new ValidationResponse(false, logs));
            }

            // Validate Savepoint Path (if provided)
            if (request.getSavepointPath() != null && !request.getSavepointPath().isEmpty()) {
                logs.add("Validating Savepoint Path...");
                String savepointError = validateSavepointPath(request.getSavepointPath(), logs);
                if (savepointError != null) {
                    return ResponseEntity.ok(new ValidationResponse(false, logs));
                }
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

    @GetMapping("/{jobName}")
    public ResponseEntity<com.datahondo.flink.streaming.web.model.SavedJobConfig> getJobConfig(@PathVariable String jobName) {
        java.io.File file = new java.io.File(getConfigDir() + "/" + jobName + ".json");
        if (!file.exists()) {
            return ResponseEntity.notFound().build();
        }
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.datahondo.flink.streaming.web.model.SavedJobConfig config = mapper.readValue(file, com.datahondo.flink.streaming.web.model.SavedJobConfig.class);
            return ResponseEntity.ok(config);
        } catch (Exception e) {
            log.error("Failed to read config for {}", jobName, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/{jobName}/audit")
    public ResponseEntity<List<com.datahondo.flink.streaming.audit.AuditEvent>> getJobAuditEvents(@PathVariable String jobName) {
        return ResponseEntity.ok(auditCache.getEvents(jobName));
    }

    @GetMapping("/{jobName}/reconciliation")
    public ResponseEntity<com.datahondo.flink.streaming.audit.ReconciliationReport> getJobReconciliation(@PathVariable String jobName) {
        com.datahondo.flink.streaming.audit.ReconciliationReport report = auditCache.getLatestReport(jobName);
        if (report == null) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(report);
    }

    /**
     * Triggers a savepoint for a running job without cancelling it.
     * Blocks until the savepoint completes or the configured poll timeout elapses.
     *
     * <p>The resulting savepoint path is stored in {@link SavepointRegistry} and
     * returned in the response body so it can be supplied as {@code savepointPath}
     * in a future submit request to restore the job.
     */
    @PostMapping("/{jobName}/savepoint")
    public ResponseEntity<?> triggerSavepoint(@PathVariable String jobName) {
        log.info("Received savepoint request for job: {}", jobName);
        try {
            SavepointRecord record = orchestrator.triggerSavepoint(jobName, systemConfig);
            savepointRegistry.register(jobName, record);
            return ResponseEntity.ok(record);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (SavepointException e) {
            log.error("Savepoint failed for job '{}'", jobName, e);
            return ResponseEntity.internalServerError()
                    .body("Savepoint failed: " + e.getMessage());
        }
    }

    /**
     * Returns all savepoints registered for the given job, ordered by creation time (oldest first).
     */
    @GetMapping("/{jobName}/savepoints")
    public ResponseEntity<java.util.List<SavepointRecord>> listSavepoints(
            @PathVariable String jobName) {
        return ResponseEntity.ok(savepointRegistry.getForJob(jobName));
    }

    @PostMapping("/submit")
    public ResponseEntity<String> submitJob(@RequestBody JobRequest request) {
        log.info("Received job submission request: {}", request.getJobName());
        if (request.getSources() == null || request.getSources().isEmpty()) {
            return ResponseEntity.badRequest().body("Job must have at least one source configured.");
        }
        List<String> checkpointLogs = new ArrayList<>();
        String checkpointDirError = validateCheckpointDir(request.getCheckpointDir(), checkpointLogs);
        if (checkpointDirError != null) {
            return ResponseEntity.badRequest().body(checkpointDirError);
        }
        if (request.getSavepointPath() != null && !request.getSavepointPath().isEmpty()) {
            List<String> savepointLogs = new ArrayList<>();
            String savepointError = validateSavepointPath(request.getSavepointPath(), savepointLogs);
            if (savepointError != null) {
                return ResponseEntity.badRequest().body(savepointError);
            }
        }
        try {
            StreamingJobConfig config = mapToConfig(request);

            // Validate Kafka source and target connectivity before submitting to Flink —
            // catches missing topics, auth failures, and broker unreachability early.
            for (com.datahondo.flink.streaming.config.SourceConfig source : config.getSources()) {
                try {
                    validatorService.validateConnection(source.getKafka());
                } catch (Exception e) {
                    return ResponseEntity.badRequest().body(
                            "Source topic '" + source.getKafka().getTopic()
                                    + "' validation failed: " + e.getMessage());
                }
            }
            try {
                validatorService.validateConnection(config.getTarget().getKafka());
            } catch (Exception e) {
                return ResponseEntity.badRequest().body(
                        "Target topic '" + config.getTarget().getKafka().getTopic()
                                + "' validation failed: " + e.getMessage());
            }

            String configPath = getConfigDir() + "/" + request.getJobName() + ".json";
            JobAuditRecord auditRecord = jobAuditService.createRecord(
                    request.getJobName(), null,
                    request.getParallelism(), request.getCheckpointInterval(),
                    configPath, request);
            try {
                org.apache.flink.core.execution.JobClient jobClient = orchestrator.submitJob(config);
                saveJobConfig(request);
                String flinkJobId = jobClient != null ? jobClient.getJobID().toString() : null;
                jobAuditService.updateRunning(auditRecord.getId(), flinkJobId);
                if (jobClient != null) {
                    final org.apache.flink.core.execution.JobClient ref = jobClient;
                    jobStatusPoller.register(request.getJobName(), () -> {
                        try { return ref.getJobStatus().get().name(); } catch (Exception ex) { return null; }
                    });
                }
            } catch (Exception ex) {
                jobAuditService.updateStatus(auditRecord.getId(),
                        JobAuditRecord.Status.FAILED, ex.getMessage());
                throw ex;
            }
            return ResponseEntity.ok("Job '" + request.getJobName() + "' submitted successfully.");
        } catch (Exception e) {
            log.error("Failed to submit job", e);
            return ResponseEntity.internalServerError().body("Failed to submit job: " + e.getMessage());
        }
    }

    /**
     * Validates a savepoint path URI format and, for local {@code file:///} URIs, verifies
     * the directory exists on this host.
     *
     * @return an error string if validation fails, {@code null} if the path is valid or absent.
     */
    private String validateSavepointPath(String savepointPath, List<String> logs) {
        if (savepointPath == null || savepointPath.isEmpty()) {
            return null;
        }
        if (savepointPath.matches("(?i)file:/[^/].*") || savepointPath.matches("(?i)file://[^/].*")) {
            String msg = "❌ Invalid savepoint path '" + savepointPath
                    + "': local paths require three slashes, e.g. file:///data/savepoints";
            logs.add(msg);
            return msg;
        }
        logs.add("✅ Savepoint path URI format OK");

        if (savepointPath.toLowerCase().startsWith("file:///")) {
            String localPath = savepointPath.substring("file://".length());

            if (localPath.contains("..")) {
                String msg = "❌ Invalid savepoint path (directory traversal detected): " + localPath;
                logs.add(msg);
                return msg;
            }

            java.io.File dir = new java.io.File(localPath);
            try {
                dir.getCanonicalPath();
            } catch (java.io.IOException e) {
                String msg = "❌ Failed to resolve savepoint path: " + e.getMessage();
                logs.add(msg);
                return msg;
            }

            if (!dir.exists()) {
                String msg = "❌ Savepoint path does not exist: " + localPath;
                logs.add(msg);
                return msg;
            }
            if (!dir.isDirectory()) {
                String msg = "❌ Savepoint path is not a directory: " + localPath;
                logs.add(msg);
                return msg;
            }
            logs.add("✅ Savepoint path exists: " + localPath
                    + " (ensure the same path is mounted in all Flink containers)");
        } else {
            logs.add("✅ Savepoint path existence check skipped for non-local URI (hdfs/s3/etc.)");
        }
        return null;
    }

    /**
     * Validates the checkpoint directory URI format and local path existence.
     * Appends informational messages to {@code logs}.
     *
     * @return an error string if validation fails, {@code null} if the directory is valid.
     */
    private String validateCheckpointDir(String checkpointDir, List<String> logs) {
        if (checkpointDir == null || checkpointDir.isEmpty()) {
            logs.add("✅ Checkpoint directory: using cluster default");
            return null;
        }
        if (checkpointDir.matches("(?i)file:/[^/].*") || checkpointDir.matches("(?i)file://[^/].*")) {
            String msg = "❌ Invalid checkpoint directory '" + checkpointDir
                    + "': local paths require three slashes, e.g. file:///tmp/checkpoints";
            logs.add(msg);
            return msg;
        }
        logs.add("✅ Checkpoint directory URI format OK");
        if (checkpointDir.toLowerCase().startsWith("file:///")) {
            // strip "file://" to get the absolute local path (e.g. file:///tmp → /tmp)
            String localPath = checkpointDir.substring("file://".length());

            // Security Check: prevent directory traversal
            if (localPath.contains("..")) {
                String msg = "❌ Invalid checkpoint directory (directory traversal detected): " + localPath;
                logs.add(msg);
                return msg;
            }

            java.io.File dir = new java.io.File(localPath);

            try {
                // Ensure it resolves to an absolute canonical path (mitigates symlink traversal if needed)
                String canonicalPath = dir.getCanonicalPath();
                if (!dir.isAbsolute()) {
                     String msg = "❌ Checkpoint directory must be an absolute path: " + localPath;
                     logs.add(msg);
                     return msg;
                }
            } catch (java.io.IOException e) {
                String msg = "❌ Failed to resolve canonical path for checkpoint directory: " + e.getMessage();
                logs.add(msg);
                return msg;
            }

            if (!dir.exists()) {
                String msg = "❌ Checkpoint directory does not exist: " + localPath;
                logs.add(msg);
                return msg;
            }
            if (!dir.isDirectory()) {
                String msg = "❌ Checkpoint path is not a directory: " + localPath;
                logs.add(msg);
                return msg;
            }
            logs.add("✅ Checkpoint directory exists on this host: " + localPath
                    + " (ensure the same path is also mounted in the Flink jobmanager/taskmanager containers)");
        } else {
            logs.add("✅ Checkpoint directory existence check skipped for non-local URI (hdfs/s3/etc.)");
        }
        return null;
    }

    private String getConfigDir() {
        if (systemConfig.getFlink() != null
                && systemConfig.getFlink().getConfigDir() != null
                && !systemConfig.getFlink().getConfigDir().isEmpty()) {
            return systemConfig.getFlink().getConfigDir();
        }
        return "configs";
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
            
            String fileName = getConfigDir() + "/" + request.getJobName() + ".json";
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

        // Savepoint restore (optional — only set when user provides a savepoint path)
        if (request.getSavepointPath() != null && !request.getSavepointPath().isEmpty()) {
            flink.setSavepointPath(request.getSavepointPath());
            flink.setAllowNonRestoredState(
                    Boolean.TRUE.equals(request.getAllowNonRestoredState()));
            log.info("Job '{}' will restore from savepoint: {}",
                    request.getJobName(), request.getSavepointPath());
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
