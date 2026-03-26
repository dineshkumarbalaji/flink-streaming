package com.datahondo.flink.streaming.transformation;

import com.datahondo.flink.streaming.config.TransformationConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
@Component
public class TransformationLayer {
    
    public Table applyTransformation(
            StreamTableEnvironment tableEnv,
            TransformationConfig config) throws Exception {
        
        String sql;
        String type = config.getType();
        if ("FILE".equalsIgnoreCase(type)) {
            if (config.getSqlFilePath() == null || config.getSqlFilePath().isEmpty()) {
                throw new IllegalArgumentException("Transformation type is FILE but sqlFilePath is not set");
            }
            log.info("Loading SQL from file: {}", config.getSqlFilePath());
            sql = new String(Files.readAllBytes(Paths.get(config.getSqlFilePath())));
        } else if ("INLINE".equalsIgnoreCase(type) || type == null) {
            if (config.getSqlContent() == null || config.getSqlContent().isEmpty()) {
                if (config.getSqlFilePath() != null && !config.getSqlFilePath().isEmpty()) {
                    log.info("sqlContent empty, falling back to file: {}", config.getSqlFilePath());
                    sql = new String(Files.readAllBytes(Paths.get(config.getSqlFilePath())));
                } else {
                    throw new IllegalArgumentException("No SQL provided: set type=INLINE with sqlContent, or type=FILE with sqlFilePath");
                }
            } else {
                log.info("Using inline SQL content");
                sql = config.getSqlContent();
            }
        } else {
            throw new IllegalArgumentException("Unknown transformation type: " + type + ". Must be INLINE or FILE");
        }
        
        log.info("Executing transformation SQL:\n{}", sql);
        
        // Execute SQL query
        Table resultTable = tableEnv.sqlQuery(sql);
        
        // Create temporary view for the result
        tableEnv.createTemporaryView(config.getResultTableName(), resultTable);
        
        log.info("Transformation completed. Result table: {}", config.getResultTableName());
        return resultTable;
    }
}