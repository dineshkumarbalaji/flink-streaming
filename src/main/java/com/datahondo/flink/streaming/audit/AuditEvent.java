package com.datahondo.flink.streaming.audit;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

/**
 * Immutable record of a single audit point in the job pipeline.
 *
 * <p>An {@code AuditEvent} is emitted by the orchestrator or individual layers at
 * key stages (source read, validation rejection, transformation output, target write,
 * job lifecycle). All events share the same {@code runId} for a given job execution,
 * enabling downstream aggregation and lineage queries.
 *
 * <p>Usage example:
 * <pre>
 *   AuditEvent.builder()
 *       .runId(runId)
 *       .jobName("orders-enrichment")
 *       .eventType(AuditEventType.SOURCE_READ)
 *       .stage("source/orders-topic")
 *       .count(1024L)
 *       .timestamp(Instant.now())
 *       .build();
 * </pre>
 */
@Data
@Builder
public class AuditEvent {

    /** Unique identifier for this job execution (UUID prefix + epoch-ms suffix). */
    private final String runId;

    /** Logical job name from {@code StreamingJobConfig}. */
    private final String jobName;

    /** What happened at this audit point. */
    private final AuditEventType eventType;

    /**
     * Human-readable stage label that identifies where in the pipeline
     * this event originated.  Convention: {@code "source/<topic>"},
     * {@code "transformation"}, {@code "target/<topic>"}.
     */
    private final String stage;

    /** Record count associated with this event (0 for lifecycle events). */
    @Builder.Default
    private final long count = 0L;

    /** Wall-clock time at which the event was created. */
    @Builder.Default
    private final Instant timestamp = Instant.now();

    /**
     * Arbitrary key-value metadata (e.g. Flink job-id, Kafka offset,
     * schema version, environment tag).
     */
    private final Map<String, String> metadata;
}
