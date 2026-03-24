package com.datahondo.flink.streaming.config;

import lombok.Data;

@Data
public class WatermarkConfig {
    private String strategy; // BOUNDED, PERIODIC
    private String mode; // PROCESS_TIME, EXISTING
    private String timestampColumn;
    private long maxOutOfOrderness; // milliseconds
    private String timestampFormat; // optional, e.g., "yyyy-MM-dd HH:mm:ss"
}