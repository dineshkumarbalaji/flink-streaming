package com.datahondo.flink.streaming.config;

import lombok.Data;

@Data
public class SourceConfig {
    private KafkaConfig kafka;
    private String tableName;
    /**
     * Optional SQL alias used in multi-source JOIN queries.
     * e.g. alias="s1" → SELECT * FROM source_table s1 JOIN source_table2 s2 ON s1.id = s2.id
     */
    private String alias;
    private SchemaConfig schema;
    private WatermarkConfig watermark;
}