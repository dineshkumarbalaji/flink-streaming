package com.datahondo.flink.streaming.web.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class SqlValidatorService {

    /**
     * Schema and watermark metadata for a single source table used during SQL validation.
     */
    public static final class SourceEntry {
        public final String tableName;
        public final String schema;
        public final boolean enableWatermark;
        public final String watermarkMode;

        public SourceEntry(String tableName, String schema,
                           boolean enableWatermark, String watermarkMode) {
            this.tableName = tableName;
            this.schema = schema;
            this.enableWatermark = enableWatermark;
            this.watermarkMode = watermarkMode;
        }
    }

    /**
     * Validates the SQL query against all provided source table schemas.
     * All source tables are registered in a single Flink table environment so that
     * multi-source JOINs are fully validated, not just the first source.
     */
    public void validateSql(String sqlQuery, List<SourceEntry> sources) throws Exception {
        if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
            throw new Exception("SQL Query cannot be empty");
        }

        List<SourceEntry> effectiveSources = (sources != null && !sources.isEmpty())
                ? sources
                : Collections.singletonList(new SourceEntry("source_table", null, false, null));

        log.info("Validating SQL against {} source table(s)", effectiveSources.size());

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            for (SourceEntry source : effectiveSources) {
                String tableName = (source.tableName != null && !source.tableName.isEmpty())
                        ? source.tableName : "source_table";

                if (source.schema != null && !source.schema.isEmpty()) {
                    String ddl = buildDdlFromSchema(tableName, source.schema,
                            source.enableWatermark, source.watermarkMode);
                    log.info("Registering validation table '{}' with DDL: {}", tableName, ddl);
                    tableEnv.executeSql(ddl);
                } else {
                    DataStream<String> dummyStream = env.fromElements("dummy_data");
                    Table sourceTable = tableEnv.fromDataStream(dummyStream);
                    tableEnv.createTemporaryView(tableName, sourceTable);
                }
            }

            tableEnv.sqlQuery(sqlQuery);
            log.info("SQL Validation successful");
        } catch (Exception e) {
            log.warn("SQL Validation failed: {}", e.getMessage());
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new Exception("Invalid SQL: " + cause.getMessage());
        }
    }

    private String buildDdlFromSchema(String tableName, String jsonSchema,
                                      boolean enableWatermark, String watermarkMode) throws Exception {
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
            String colType = "STRING";

            JsonNode typeNode = entry.getValue().get("type");
            if (typeNode != null) {
                String typeStr = typeNode.asText().toLowerCase();
                switch (typeStr) {
                    case "integer": colType = "INT"; break;
                    case "number":  colType = "DOUBLE"; break;
                    case "boolean": colType = "BOOLEAN"; break;
                    default:        colType = "STRING";
                }
            }

            if (!first) columns.append(", ");
            columns.append("`").append(colName).append("` ").append(colType);
            first = false;
        }

        if (enableWatermark && "PROCESS_TIME".equalsIgnoreCase(watermarkMode)) {
            if (!first) columns.append(", ");
            columns.append("`processed_time` AS PROCTIME()");
        }

        return String.format(
            "CREATE TEMPORARY TABLE %s (%s) WITH ('connector' = 'datagen', 'number-of-rows' = '1')",
            tableName, columns.toString()
        );
    }
}
