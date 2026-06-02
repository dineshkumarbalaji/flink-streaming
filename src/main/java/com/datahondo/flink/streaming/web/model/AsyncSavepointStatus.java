package com.datahondo.flink.streaming.web.model;

import lombok.Builder;
import lombok.Data;
import java.time.Instant;

/**
 * Response returned immediately when an async savepoint is triggered.
 * The client polls GET /api/jobs/{jobName}/savepoints for the completed record.
 */
@Data
@Builder
public class AsyncSavepointStatus {

    public enum State { PENDING, COMPLETED, FAILED }

    private final String jobName;
    private final String message;
    private final State state;
    @Builder.Default
    private final Instant triggeredAt = Instant.now();
}
