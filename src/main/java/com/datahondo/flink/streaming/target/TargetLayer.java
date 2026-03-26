package com.datahondo.flink.streaming.target;

import com.datahondo.flink.streaming.config.TargetConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Contract for all target (sink) layer implementations.
 * Implementations consume a result {@link Table} and write it to an external system.
 */
public interface TargetLayer {

    /**
     * Returns the sink type token this implementation handles (e.g. "KAFKA", "JDBC").
     */
    String getSinkType();

    /**
     * Wires the result table into the appropriate sink.
     *
     * @param tableEnv    the table environment
     * @param resultTable the transformed table to sink
     * @param config      target-specific configuration
     */
    void sink(StreamTableEnvironment tableEnv, Table resultTable, TargetConfig config);
}
