package com.datahondo.flink.streaming.savepoint;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * Immutable snapshot of a completed savepoint operation.
 */
@Data
@Builder
public class SavepointRecord {
    /** Logical job name used by this application. */
    private final String jobName;
    /** Flink-assigned job ID (UUID hex string). */
    private final String jobId;
    /** Absolute path to the savepoint directory written by Flink. */
    private final String savepointPath;
    /** Timestamp when the savepoint completed. */
    private final Instant createdAt;
    /** True if the job was cancelled as part of this savepoint operation. */
    private final boolean cancelledJob;
}
