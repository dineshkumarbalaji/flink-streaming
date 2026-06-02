package com.datahondo.flink.streaming.web;

import com.datahondo.flink.streaming.job.StreamingJobOrchestrator;
import com.datahondo.flink.streaming.job.audit.JobAuditRecord;
import com.datahondo.flink.streaming.job.audit.JobAuditService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

/**
 * Feature 008 — REST API for the job audit dashboard.
 *
 * <p>Provides persistent job history, live status, and stop controls.
 * All endpoints are read-heavy; writes go through {@link JobAuditService}.
 */
@Slf4j
@RestController
@RequestMapping("/api/dashboard/jobs")
@RequiredArgsConstructor
public class JobDashboardController {

    private final JobAuditService auditService;
    private final StreamingJobOrchestrator orchestrator;

    @GetMapping
    public ResponseEntity<List<JobAuditRecord>> listAllJobs() {
        return ResponseEntity.ok(auditService.findAll());
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobAuditRecord> getJob(@PathVariable Long id) {
        return auditService.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/by-name/{jobName}")
    public ResponseEntity<List<JobAuditRecord>> getByJobName(@PathVariable String jobName) {
        return ResponseEntity.ok(auditService.findByJobName(jobName));
    }

    @PostMapping("/{id}/stop")
    public ResponseEntity<String> stopJob(@PathVariable Long id) {
        Optional<JobAuditRecord> opt = auditService.findById(id);
        if (!opt.isPresent()) return ResponseEntity.notFound().build();

        JobAuditRecord record = opt.get();
        if (record.getStatus() != JobAuditRecord.Status.RUNNING) {
            return ResponseEntity.status(409)
                    .body("Job is not RUNNING — current status: " + record.getStatus());
        }
        try {
            orchestrator.cancelJob(record.getJobName());
            auditService.updateStatus(id, JobAuditRecord.Status.CANCELLED, null);
            return ResponseEntity.ok("Job '" + record.getJobName() + "' cancelled.");
        } catch (Exception e) {
            log.error("Failed to stop job id={}", id, e);
            return ResponseEntity.internalServerError().body("Stop failed: " + e.getMessage());
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<String> deleteRecord(@PathVariable Long id) {
        try {
            auditService.deleteById(id);
            return ResponseEntity.ok("Audit record " + id + " deleted.");
        } catch (IllegalStateException e) {
            return ResponseEntity.status(409).body(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Delete failed: " + e.getMessage());
        }
    }
}
