package com.datahondo.flink.streaming.job.audit;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobAuditRepository extends JpaRepository<JobAuditRecord, Long> {
    List<JobAuditRecord> findByJobNameOrderBySubmittedAtDesc(String jobName);
    List<JobAuditRecord> findByStatusIn(List<JobAuditRecord.Status> statuses);
    List<JobAuditRecord> findAllByOrderBySubmittedAtDesc();
}
