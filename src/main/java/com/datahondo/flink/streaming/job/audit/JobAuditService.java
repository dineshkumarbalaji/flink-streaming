package com.datahondo.flink.streaming.job.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobAuditService {

    private final JobAuditRepository repository;

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Transactional
    public JobAuditRecord createRecord(String jobName, String runId,
                                       Integer parallelism, Long checkpointInterval,
                                       String configFilePath, Object configSnapshot) {
        String snapshot = null;
        try { snapshot = mapper.writeValueAsString(configSnapshot); } catch (Exception ignored) {}

        JobAuditRecord record = JobAuditRecord.builder()
                .jobName(jobName)
                .runId(runId)
                .parallelism(parallelism)
                .checkpointInterval(checkpointInterval)
                .configFilePath(configFilePath)
                .configSnapshot(snapshot)
                .build();
        JobAuditRecord saved = repository.save(record);
        log.info("[AUDIT-008] Created record id={} for job='{}'", saved.getId(), jobName);
        return saved;
    }

    @Transactional
    public void updateRunning(Long id, String flinkJobId) {
        repository.findById(id).ifPresent(r -> {
            r.setFlinkJobId(flinkJobId);
            r.setStatus(JobAuditRecord.Status.RUNNING);
            r.setUpdatedAt(Instant.now());
            repository.save(r);
        });
    }

    @Transactional
    public void updateStatus(Long id, JobAuditRecord.Status status, String errorMessage) {
        repository.findById(id).ifPresent(r -> {
            r.setStatus(status);
            r.setUpdatedAt(Instant.now());
            if (errorMessage != null) r.setErrorMessage(errorMessage);
            repository.save(r);
        });
    }

    public List<JobAuditRecord> findAll() {
        return repository.findAllByOrderBySubmittedAtDesc();
    }

    public Optional<JobAuditRecord> findById(Long id) {
        return repository.findById(id);
    }

    public List<JobAuditRecord> findByJobName(String jobName) {
        return repository.findByJobNameOrderBySubmittedAtDesc(jobName);
    }

    @Transactional
    public void deleteById(Long id) {
        repository.findById(id).ifPresent(r -> {
            if (r.getStatus() == JobAuditRecord.Status.RUNNING) {
                throw new IllegalStateException("Cannot delete a RUNNING job audit record");
            }
            repository.deleteById(id);
            log.info("[AUDIT-008] Deleted record id={} job='{}'", id, r.getJobName());
        });
    }
}
