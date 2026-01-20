package com.antheon.flink.streaming.transformation;

import com.antheon.flink.streaming.config.TransformationConfig;
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
        if (config.getSqlContent() != null && !config.getSqlContent().isEmpty()) {
            log.info("Using provided SQL content");
            sql = config.getSqlContent();
        } else {
            log.info("Loading SQL from file: {}", config.getSqlFilePath());
            sql = new String(Files.readAllBytes(Paths.get(config.getSqlFilePath())));
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