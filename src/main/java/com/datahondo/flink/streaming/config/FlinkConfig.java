package com.datahondo.flink.streaming.config;

import lombok.Data;

@Data
public class FlinkConfig {
    private Integer parallelism;
    private String checkpointDir;
    private Long checkpointInterval;
    private Integer maxConcurrentCheckpoints;

    // Savepoint config
    /** Directory where savepoints are written (separate from checkpoint dir). */
    private String savepointDir;
    /** How long (ms) to poll Flink REST API before declaring a savepoint timed out. Default 5 min. */
    private Long savepointPollTimeoutMs = 300_000L;

    // Per-submission restore fields (not from system YAML — set by JobController from request)
    /** Path to an existing savepoint to restore the job from. Null means fresh start. */
    private String savepointPath;
    /** When true, state from operators absent in the savepoint is dropped instead of failing. */
    private Boolean allowNonRestoredState;

    // Remote connection config
    private boolean remote = false;
    private String host = "localhost";
    private int port = 8081;
    private String jarPath;

    // State backend: HASHMAP (default, in-memory) or ROCKSDB (disk-spilling for large state)
    private String stateBackend = "HASHMAP";

    // Directory for saved job config JSON files (default: configs/)
    private String configDir = "configs";
}