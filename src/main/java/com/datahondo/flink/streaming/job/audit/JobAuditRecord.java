package com.datahondo.flink.streaming.job.audit;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.Instant;

/**
 * Persistent audit record for a single Flink job submission.
 * One row per submit call — multiple rows may exist for the same jobName
 * across restarts or resubmissions.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "job_audit_records")
public class JobAuditRecord {

    public enum Status { SUBMITTING, RUNNING, FINISHED, FAILED, CANCELLED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_name", nullable = false)
    private String jobName;

    @Column(name = "flink_job_id")
    private String flinkJobId;

    @Column(name = "run_id")
    private String runId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    @Builder.Default
    private Status status = Status.SUBMITTING;

    @Column(name = "parallelism")
    private Integer parallelism;

    @Column(name = "checkpoint_interval")
    private Long checkpointInterval;

    @Column(name = "config_file_path")
    private String configFilePath;

    @Column(name = "config_snapshot", columnDefinition = "TEXT")
    private String configSnapshot;

    @Column(name = "submitted_at")
    @Builder.Default
    private Instant submittedAt = Instant.now();

    @Column(name = "updated_at")
    @Builder.Default
    private Instant updatedAt = Instant.now();

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;
}
