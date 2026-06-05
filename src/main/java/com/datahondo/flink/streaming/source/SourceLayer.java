package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.SourceConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Contract for all source layer implementations.
 * Implementations create a Flink temporary view registered under
 * {@link SourceConfig#getTableName()} and return the resulting {@link Table}.
 *
 * <p>The {@link #getSourceType()} method is used by the orchestrator to route
 * each {@link SourceConfig} to the correct implementation.
 */
public interface SourceLayer {

    /**
     * Source type token this implementation handles (e.g. "KAFKA", "FILE", "JDBC", "API").
     * Default is "KAFKA" for backward compatibility.
     */
    default String getSourceType() {
        return "KAFKA";
    }

    /**
     * Creates and registers a source table in {@code tableEnv}.
     *
     * @param env      the streaming execution environment
     * @param tableEnv the table environment used for registration
     * @param config   source-specific configuration
     * @return the registered {@link Table}
     */
    Table createSourceTable(StreamExecutionEnvironment env,
                            StreamTableEnvironment tableEnv,
                            SourceConfig config);
}
