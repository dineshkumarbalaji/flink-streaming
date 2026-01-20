package com.antheon.flink.streaming.web.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.Map;

@Slf4j
@Service
public class SqlValidatorService {

    public void validateSql(String sqlQuery, String sourceTableName, String sourceSchema, 
                          boolean enableWatermark, String watermarkMode) throws Exception {
        if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
            throw new Exception("SQL Query cannot be empty");
        }

        log.info("Validating SQL. EnableWatermark: {}, Mode: {}, Table: {}", enableWatermark, watermarkMode, sourceTableName);

        try {
            // Create a lightweight local environment for validation
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
            
            String tableName = (sourceTableName != null && !sourceTableName.isEmpty()) ? sourceTableName : "source_table";

            // If schema is provided, create a table with that schema using DDL (DataGen)
            if (sourceSchema != null && !sourceSchema.isEmpty()) {
                String ddl = buildDdlFromSchema(tableName, sourceSchema, enableWatermark, watermarkMode);
                log.info("Registering validation table with DDL: {}", ddl);
                tableEnv.executeSql(ddl);
            } else {
                // Fallback to dummy string table if no schema
                DataStream<String> dummyStream = env.fromElements("dummy_data");
                Table sourceTable = tableEnv.fromDataStream(dummyStream);
                // If watermark specific logic needed for raw stream, it's harder to mock without generic types
                // But for now, user is likely using schema if they use SQL with columns.
                tableEnv.createTemporaryView(tableName, sourceTable);
            }

            // Attempt to parse/compile the query
            tableEnv.sqlQuery(sqlQuery);
            
            log.info("SQL Validation successful");
        } catch (Exception e) {
            log.warn("SQL Validation failed: {}", e.getMessage());
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new Exception("Invalid SQL: " + cause.getMessage());
        }
    }

    private String buildDdlFromSchema(String tableName, String jsonSchema, boolean enableWatermark, String watermarkMode) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonSchema);
        JsonNode properties = root.get("properties");

        if (properties == null || !properties.isObject()) {
            throw new Exception("Invalid JSON Schema: 'properties' field missing or invalid");
        }

        StringBuilder columns = new StringBuilder();
        Iterator<Map.Entry<String, JsonNode>> it = properties.fields();
        boolean first = true;

        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            String colName = entry.getKey();
            String colType = "STRING"; // default

            JsonNode typeNode = entry.getValue().get("type");
            if (typeNode != null) {
                String typeStr = typeNode.asText().toLowerCase();
                switch (typeStr) {
                    case "integer": colType = "INT"; break;
                    case "number": colType = "DOUBLE"; break;
                    case "boolean": colType = "BOOLEAN"; break;
                    case "string": colType = "STRING"; break;
                    default: colType = "STRING";
                }
            }

            if (!first) columns.append(", ");
            columns.append("`").append(colName).append("` ").append(colType);
            first = false;
        }

        // Add Watermark / Computed columns for validation
        if (enableWatermark && "PROCESS_TIME".equalsIgnoreCase(watermarkMode)) {
            // Add processed_time as a computed column for PROCTIME()
            // Syntax: `processed_time` AS PROCTIME()
            if (!first) columns.append(", ");
            columns.append("`processed_time` AS PROCTIME()");
        }

        // Use 'datagen' connector for validation
        return String.format(
            "CREATE TEMPORARY TABLE %s (%s) WITH ('connector' = 'datagen', 'number-of-rows' = '1')",
            tableName, columns.toString()
        );
    }
}
