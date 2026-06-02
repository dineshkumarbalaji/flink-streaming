package com.datahondo.flink.streaming.config;

import lombok.Data;

/**
 * Dead Letter Queue configuration — routes unprocessable source records to a
 * dedicated Kafka topic instead of silently dropping them.
 */
@Data
public class DlqConfig {
    /** When false, invalid records are dropped (logged only). */
    private boolean enabled = false;
    /** DLQ topic name. Defaults to {@code <source-topic>-dlq} when null. */
    private String topic;
    /** Bootstrap servers for the DLQ topic (may differ from the source broker). */
    private String bootstrapServers;
}
