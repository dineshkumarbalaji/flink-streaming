package com.datahondo.flink.streaming.config;

import lombok.Data;

@Data
public class TransformationConfig {
    /**
     * SQL source type: INLINE or FILE.
     * INLINE uses sqlContent; FILE uses sqlFilePath.
     * Defaults to INLINE if sqlContent is present, FILE otherwise.
     */
    private String type;
    private String sqlFilePath;
    private String sqlContent;
    private String resultTableName;
}