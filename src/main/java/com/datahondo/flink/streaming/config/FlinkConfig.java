package com.datahondo.flink.streaming.config;

import lombok.Data;

@Data
public class FlinkConfig {
    private Integer parallelism;
    private String checkpointDir;
    private Long checkpointInterval;
    private Integer maxConcurrentCheckpoints;
    
    // Remote connection config
    private boolean remote = false;
    private String host = "localhost";
    private int port = 8081;
    private String jarPath;
}