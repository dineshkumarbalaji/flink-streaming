package com.datahondo.flink.streaming.audit;

import lombok.Builder;
import lombok.Data;
import java.time.Instant;

/**
 * Envelope wrapping an unprocessable record routed to the Dead Letter Queue.
 * Serialised as JSON and published to the DLQ Kafka topic.
 */
@Data
@Builder
public class DlqRecord {

    public enum ErrorType {
        SCHEMA_VALIDATION, TYPE_CONVERSION, MALFORMED, SERIALIZATION
    }

    private final String originalPayload;
    private final ErrorType errorType;
    private final String errorMessage;
    private final String sourceTopic;
    private final String jobName;
    @Builder.Default
    private final Instant timestamp = Instant.now();
}
