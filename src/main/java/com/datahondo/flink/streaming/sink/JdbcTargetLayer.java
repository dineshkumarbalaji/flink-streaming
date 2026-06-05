package com.datahondo.flink.streaming.sink;

import com.datahondo.flink.streaming.config.SchemaConfig;
import com.datahondo.flink.streaming.config.TargetConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Feature 010-A — JDBC Sink (Warm zone).
 * Writes the result Table to any JDBC datasource with INSERT or upsert semantics.
 * Upsert key resolved from config.upsertKeyColumns, falling back to schema primaryKey fields.
 */
@Slf4j
@Component
public class JdbcTargetLayer implements TargetLayer {

    @Override
    public String getSinkType() {
        return "JDBC";
    }

    @Override
    public void sink(StreamTableEnvironment tableEnv, Table resultTable, TargetConfig config) {
        validateConfig(config);

        List<String> columns = resolveColumns(config);
        List<String> keyColumns = resolveKeyColumns(config, columns);

        JdbcDialect dialect = resolveDialect(config);
        String sql;
        if (config.isUpsertMode()) {
            if (keyColumns.isEmpty()) {
                throw new IllegalArgumentException(
                        "[JDBC-SINK] upsertMode=true but no upsert key could be resolved. "
                        + "Set target.upsertKeyColumns or mark schema fields with primaryKey=true");
            }
            sql = dialect.upsertSql(config.getTableName(), columns, keyColumns);
            log.info("[JDBC-SINK] Upsert mode — dialect={} keys={} sql={}",
                    dialect.name(), keyColumns, sql);
        } else {
            sql = dialect.insertSql(config.getTableName(), columns);
            log.info("[JDBC-SINK] Insert mode — table={} sql={}", config.getTableName(), sql);
        }

        // Register as Flink Table JDBC connector via DDL
        String ddl = buildSinkDdl(config, columns, keyColumns);
        log.debug("[JDBC-SINK] Sink DDL:\n{}", ddl);
        tableEnv.executeSql(ddl);
        resultTable.executeInsert(config.getTableName());
    }

    private String buildSinkDdl(TargetConfig config, List<String> columns, List<String> keyColumns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(config.getTableName()).append("` (\n");
        List<SchemaConfig.SchemaField> fields = config.getSchema() != null
                ? config.getSchema().getFields() : null;
        if (fields != null && !fields.isEmpty()) {
            for (int i = 0; i < fields.size(); i++) {
                SchemaConfig.SchemaField f = fields.get(i);
                sb.append("  `").append(f.getName()).append("` ").append(f.getType());
                if (!f.isNullable()) sb.append(" NOT NULL");
                if (i < fields.size() - 1) sb.append(",");
                sb.append("\n");
            }
        }
        if (!keyColumns.isEmpty()) {
            sb.append("  ,PRIMARY KEY (")
              .append(String.join(", ", keyColumns))
              .append(") NOT ENFORCED\n");
        }
        sb.append(") WITH (\n");
        sb.append("  'connector' = 'jdbc',\n");
        sb.append("  'url' = '").append(buildJdbcUrl(config)).append("',\n");
        sb.append("  'table-name' = '").append(config.getTableName()).append("',\n");
        sb.append("  'sink.buffer-flush.max-rows' = '").append(config.getBatchSize()).append("',\n");
        sb.append("  'sink.buffer-flush.interval' = '").append(config.getBatchIntervalMs()).append("ms'");
        if (config.getJdbcUsername() != null && !config.getJdbcUsername().isEmpty()) {
            sb.append(",\n  'username' = '").append(config.getJdbcUsername()).append("'");
            sb.append(",\n  'password' = '")
              .append(config.getJdbcPassword() != null ? config.getJdbcPassword() : "")
              .append("'");
        }
        sb.append("\n)");
        return sb.toString();
    }

    private String buildJdbcUrl(TargetConfig config) {
        String url = config.getJdbcUrl();
        if ("require".equalsIgnoreCase(config.getSslMode())
                || "verify-full".equalsIgnoreCase(config.getSslMode())) {
            String sep = url.contains("?") ? "&" : "?";
            url = url + sep + "ssl=true";
        }
        return url;
    }

    private List<String> resolveColumns(TargetConfig config) {
        List<String> cols = new ArrayList<>();
        if (config.getSchema() != null && config.getSchema().hasFields()) {
            for (SchemaConfig.SchemaField f : config.getSchema().getFields()) {
                cols.add(f.getName());
            }
        }
        return cols;
    }

    private List<String> resolveKeyColumns(TargetConfig config, List<String> allColumns) {
        // 1. Explicit config list takes priority
        if (config.getUpsertKeyColumns() != null && !config.getUpsertKeyColumns().isEmpty()) {
            return new ArrayList<>(config.getUpsertKeyColumns());
        }
        // 2. Fall back to schema fields marked primaryKey=true
        List<String> keys = new ArrayList<>();
        if (config.getSchema() != null && config.getSchema().hasFields()) {
            for (SchemaConfig.SchemaField f : config.getSchema().getFields()) {
                if (f.isPrimaryKey()) keys.add(f.getName());
            }
        }
        return keys;
    }

    private JdbcDialect resolveDialect(TargetConfig config) {
        if (config.getJdbcDialect() != null && !config.getJdbcDialect().isEmpty()) {
            try {
                return JdbcDialect.valueOf(config.getJdbcDialect().toUpperCase());
            } catch (IllegalArgumentException ignored) {}
        }
        return JdbcDialect.detect(config.getJdbcUrl());
    }

    private void validateConfig(TargetConfig config) {
        if (config.getJdbcUrl() == null || config.getJdbcUrl().trim().isEmpty()) {
            throw new IllegalArgumentException("[JDBC-SINK] jdbcUrl must be set");
        }
        if (config.getTableName() == null || config.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("[JDBC-SINK] tableName must be set");
        }
    }
}
