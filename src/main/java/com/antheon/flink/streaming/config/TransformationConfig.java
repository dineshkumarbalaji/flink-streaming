package com.antheon.flink.streaming.config;

import lombok.Data;

@Data
public class TransformationConfig {
    private String sqlFilePath;
    private String sqlContent;
    private String resultTableName;
}