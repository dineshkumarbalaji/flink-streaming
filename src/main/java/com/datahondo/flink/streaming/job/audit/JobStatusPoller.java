package com.datahondo.flink.streaming.job.audit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Polls live Flink job status every 30 s and updates the {@link JobAuditRecord} table.
 *
 * <p>Each running job registers a {@link Callable} that returns the current Flink
 * status string (via {@code JobClient.getJobStatus()}). The poller calls these
 * callbacks on the scheduler thread and writes status transitions back through
 * {@link JobAuditService}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JobStatusPoller {

    private final JobAuditService auditService;

    private final Map<String, Callable<String>> statusProviders = new ConcurrentHashMap<>();

    public void register(String jobName, Callable<String> provider) {
        statusProviders.put(jobName, provider);
        log.debug("[POLLER] Registered status provider for job '{}'", jobName);
    }

    public void unregister(String jobName) {
        statusProviders.remove(jobName);
    }

    @Scheduled(fixedRateString = "${streaming.audit.poll-interval-ms:30000}")
    public void poll() {
        if (statusProviders.isEmpty()) return;

        List<JobAuditRecord> active = auditService.findAll().stream()
                .filter(r -> r.getStatus() == JobAuditRecord.Status.SUBMITTING
                          || r.getStatus() == JobAuditRecord.Status.RUNNING)
                .collect(Collectors.toList());

        for (JobAuditRecord record : active) {
            Callable<String> provider = statusProviders.get(record.getJobName());
            if (provider == null) continue;
            try {
                String flinkStatus = provider.call();
                JobAuditRecord.Status mapped = mapFlinkStatus(flinkStatus);
                if (mapped != record.getStatus()) {
                    String error = mapped == JobAuditRecord.Status.FAILED ? "Job failed in Flink" : null;
                    auditService.updateStatus(record.getId(), mapped, error);
                    log.info("[POLLER] Job '{}' {} → {}", record.getJobName(), record.getStatus(), mapped);
                    if (isTerminal(mapped)) {
                        unregister(record.getJobName());
                    }
                }
            } catch (Exception e) {
                log.debug("[POLLER] Could not poll '{}': {}", record.getJobName(), e.getMessage());
            }
        }
    }

    private boolean isTerminal(JobAuditRecord.Status s) {
        return s == JobAuditRecord.Status.FINISHED
            || s == JobAuditRecord.Status.FAILED
            || s == JobAuditRecord.Status.CANCELLED;
    }

    private JobAuditRecord.Status mapFlinkStatus(String s) {
        if (s == null) return JobAuditRecord.Status.RUNNING;
        switch (s.toUpperCase()) {
            case "INITIALIZING": case "CREATED": return JobAuditRecord.Status.SUBMITTING;
            case "FINISHED":                     return JobAuditRecord.Status.FINISHED;
            case "FAILED": case "SUSPENDED":     return JobAuditRecord.Status.FAILED;
            case "CANCELED":                     return JobAuditRecord.Status.CANCELLED;
            default:                             return JobAuditRecord.Status.RUNNING;
        }
    }
}
