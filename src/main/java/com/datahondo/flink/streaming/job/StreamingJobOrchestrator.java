package com.datahondo.flink.streaming.job;

import com.datahondo.flink.streaming.audit.AuditAccumulators;
import com.datahondo.flink.streaming.audit.AuditEventType;
import com.datahondo.flink.streaming.audit.AuditService;
import com.datahondo.flink.streaming.audit.ReconciliationService;
import com.datahondo.flink.streaming.audit.RunContext;
import com.datahondo.flink.streaming.config.SourceConfig;
import com.datahondo.flink.streaming.config.StreamingJobConfig;
import com.datahondo.flink.streaming.source.SourceLayer;
import com.datahondo.flink.streaming.target.TargetLayer;
import com.datahondo.flink.streaming.transformation.TransformationLayer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Orchestrates the three-layer Flink pipeline (Source → Transformation → Target)
 * and integrates the audit + reconciliation lifecycle.
 *
 * <p>Audit flow per job execution:
 * <ol>
 *   <li>Generate a unique {@link RunContext} with a scoped runId.</li>
 *   <li>Initialise the {@link AuditService} sink for this run.</li>
 *   <li>Emit {@code JOB_SUBMITTED} before graph construction.</li>
 *   <li>Build and submit the Flink graph; emit {@code JOB_RUNNING}.</li>
 *   <li>Schedule a background completion monitor that emits the final
 *       lifecycle event and triggers reconciliation once the job finishes
 *       or is cancelled.</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StreamingJobOrchestrator {

    private final SourceLayer sourceLayer;
    private final TransformationLayer transformationLayer;
    private final TargetLayer targetLayer;
    private final AuditService auditService;
    private final ReconciliationService reconciliationService;

    /** In-flight jobs: jobName → (JobClient, RunContext, windowStart). */
    private final Map<String, JobClient>  runningJobs    = new LinkedHashMap<>();
    private final Map<String, RunContext> runContexts    = new ConcurrentHashMap<>();
    private final Map<String, Instant>   windowStarts   = new ConcurrentHashMap<>();

    /** Single-threaded daemon for completion monitoring — keeps it lightweight. */
    private final ExecutorService completionMonitor =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "audit-completion-monitor");
                t.setDaemon(true);
                return t;
            });

    // ── Public API ────────────────────────────────────────────────────────────

    public List<Map<String, String>> getRunningJobs() {
        List<Map<String, String>> result = new ArrayList<>();
        runningJobs.forEach((name, client) -> {
            Map<String, String> info = new LinkedHashMap<>();
            info.put("jobName", name);
            info.put("jobId", client.getJobID().toString());
            info.put("runId", runContexts.containsKey(name)
                    ? runContexts.get(name).getRunId() : "n/a");
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

        // ── 1. Create RunContext (generates runId) ────────────────────────────
        RunContext runCtx = RunContext.create(
                config.getJobName(),
                config.getAudit(),
                config.getReconciliation());
        log.info("RunId assigned: {}", runCtx.getRunId());

        // ── 2. Init audit sink for this run ───────────────────────────────────
        auditService.initRun(runCtx);
        Instant windowStart = Instant.now();

        // ── 3. Emit JOB_SUBMITTED ─────────────────────────────────────────────
        auditService.emitLifecycle(runCtx.getRunId(), config.getJobName(),
                AuditEventType.JOB_SUBMITTED,
                buildJobMeta(config, runCtx));

        try {
            // ── 4. Build Flink environment ────────────────────────────────────
            StreamExecutionEnvironment env = buildFlinkEnvironment(config);
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
            configureFlinkEnvironment(env, config);

            // ── 5. Layer 1: Source ────────────────────────────────────────────
            log.info("=== Layer 1: Source Layer ===");
            if (config.getSources() != null) {
                for (SourceConfig sourceConfig : config.getSources()) {
                    sourceLayer.createSourceTable(env, tableEnv, sourceConfig);
                }
            }

            // ── 6. Layer 2: Transformation ────────────────────────────────────
            log.info("=== Layer 2: Transformation Layer ===");
            Table transformedTable = transformationLayer.applyTransformation(
                    tableEnv, config.getTransformation());

            // ── 7. Layer 3: Target ────────────────────────────────────────────
            log.info("=== Layer 3: Target Layer ===");
            targetLayer.sink(tableEnv, transformedTable, config.getTarget());

            // ── 8. Submit async ───────────────────────────────────────────────
            log.info("Submitting Flink job async...");
            JobClient jobClient = env.executeAsync(config.getJobName());

            runningJobs.put(config.getJobName(), jobClient);
            runContexts.put(config.getJobName(), runCtx);
            windowStarts.put(config.getJobName(), windowStart);

            // ── 9. Emit JOB_RUNNING ───────────────────────────────────────────
            Map<String, String> runningMeta = new HashMap<>(buildJobMeta(config, runCtx));
            runningMeta.put("flinkJobId", jobClient.getJobID().toString());
            auditService.emitLifecycle(runCtx.getRunId(), config.getJobName(),
                    AuditEventType.JOB_RUNNING, runningMeta);

            // ── 10. Schedule background completion monitor ────────────────────
            scheduleCompletionMonitor(config.getJobName(), jobClient, runCtx, windowStart);

            return jobClient;

        } catch (Exception e) {
            // Emit JOB_FAILED and clean up
            Map<String, String> failMeta = buildJobMeta(config, runCtx);
            failMeta.put("error", e.getMessage());
            auditService.emitLifecycle(runCtx.getRunId(), config.getJobName(),
                    AuditEventType.JOB_FAILED, failMeta);
            auditService.closeRun(runCtx.getRunId());
            throw e;
        }
    }

    public void cancelJob(String jobName) throws Exception {
        JobClient client = runningJobs.get(jobName);
        if (client == null) {
            throw new IllegalArgumentException("No running job found with name: " + jobName);
        }
        RunContext runCtx = runContexts.get(jobName);
        client.cancel().get();
        runningJobs.remove(jobName);

        if (runCtx != null) {
            auditService.emitLifecycle(runCtx.getRunId(), jobName,
                    AuditEventType.JOB_CANCELLED, null);
            triggerReconciliation(jobName, client, runCtx,
                    windowStarts.getOrDefault(jobName, Instant.now()));
            cleanup(jobName, runCtx.getRunId());
        }
        log.info("Job '{}' cancelled successfully", jobName);
    }

    // ── Background completion monitor ─────────────────────────────────────────

    private void scheduleCompletionMonitor(String jobName, JobClient client,
                                            RunContext runCtx, Instant windowStart) {
        completionMonitor.submit(() -> {
            try {
                // Block until the job terminates (works for both finite and cancelled jobs)
                client.getJobExecutionResult().get();
                log.info("[AUDIT] Job '{}' completed. Emitting JOB_COMPLETED.", jobName);
                auditService.emitLifecycle(runCtx.getRunId(), jobName,
                        AuditEventType.JOB_COMPLETED, null);
            } catch (Exception ex) {
                log.warn("[AUDIT] Job '{}' ended with error: {}", jobName, ex.getMessage());
                Map<String, String> meta = new HashMap<>();
                meta.put("error", ex.getMessage());
                auditService.emitLifecycle(runCtx.getRunId(), jobName,
                        AuditEventType.JOB_FAILED, meta);
            } finally {
                triggerReconciliation(jobName, client, runCtx, windowStart);
                cleanup(jobName, runCtx.getRunId());
            }
        });
    }

    private void triggerReconciliation(String jobName, JobClient client,
                                        RunContext runCtx, Instant windowStart) {
        if (!runCtx.isReconciliationEnabled()) return;
        try {
            Map<String, Object> accumulators = client.getAccumulators().get();
            // Derive per-source accumulator name from the first source table, or fall back
            // to the aggregated all-source key used by MetricReportingMapFunction.
            String sourceReadKey = AuditAccumulators.SOURCE_READ_ALL;
            String sourceRejectedKey = AuditAccumulators.SOURCE_REJECTED_ALL;
            String targetWrittenKey = AuditAccumulators.TARGET_WRITTEN_ALL;

            ReconciliationService.Counts counts = ReconciliationService.countsFromAccumulators(
                    accumulators, sourceReadKey, sourceRejectedKey, targetWrittenKey);

            log.info("[RECONCILIATION] Job '{}' counts: {}", jobName, counts);

            reconciliationService.reconcile(runCtx, windowStart, counts);

        } catch (Exception e) {
            log.warn("[RECONCILIATION] Failed to read accumulators for job '{}': {}",
                    jobName, e.getMessage());
        }
    }

    private void cleanup(String jobName, String runId) {
        runningJobs.remove(jobName);
        runContexts.remove(jobName);
        windowStarts.remove(jobName);
        auditService.closeRun(runId);
    }

    // ── Flink environment ─────────────────────────────────────────────────────

    private StreamExecutionEnvironment buildFlinkEnvironment(StreamingJobConfig config) {
        if (config.getFlink().isRemote()) {
            log.info("Creating REMOTE Flink environment: {}:{}",
                    config.getFlink().getHost(), config.getFlink().getPort());
            return StreamExecutionEnvironment.createRemoteEnvironment(
                    config.getFlink().getHost(),
                    config.getFlink().getPort(),
                    config.getFlink().getJarPath());
        }
        log.info("Creating LOCAL Flink environment");
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private void configureFlinkEnvironment(StreamExecutionEnvironment env,
                                            StreamingJobConfig config) {
        if (config.getFlink().getParallelism() != null) {
            env.setParallelism(config.getFlink().getParallelism());
        }
        if (config.getFlink().getCheckpointInterval() != null) {
            env.enableCheckpointing(config.getFlink().getCheckpointInterval());
        }
        if (config.getFlink().getMaxConcurrentCheckpoints() != null) {
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(
                    config.getFlink().getMaxConcurrentCheckpoints());
        }
        if (config.getFlink().getCheckpointDir() != null) {
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(config.getFlink().getCheckpointDir());
        }
        log.info("Flink environment configured — parallelism={}, checkpoint={}ms",
                config.getFlink().getParallelism(),
                config.getFlink().getCheckpointInterval());
    }

    // ── Metadata helpers ──────────────────────────────────────────────────────

    private Map<String, String> buildJobMeta(StreamingJobConfig config, RunContext runCtx) {
        Map<String, String> meta = new HashMap<>();
        meta.put("runId", runCtx.getRunId());
        meta.put("jobName", config.getJobName());
        meta.put("parallelism", String.valueOf(
                config.getFlink() != null ? config.getFlink().getParallelism() : 1));
        return meta;
    }
}
