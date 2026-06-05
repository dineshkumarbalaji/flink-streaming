package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.SchemaConfig;
import com.datahondo.flink.streaming.config.SourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Feature 009-B — JDBC / DB Source.
 * Reads from any JDBC datasource (PostgreSQL, MySQL, Oracle) via a full SELECT query.
 * Uses Flink Table API JDBC connector DDL — compatible with flink-connector-jdbc 3.x.
 */
@Slf4j
@Component
public class JdbcSourceLayer implements SourceLayer {

    @Override
    public String getSourceType() {
        return "JDBC";
    }

    @Override
    public Table createSourceTable(StreamExecutionEnvironment env,
                                   StreamTableEnvironment tableEnv,
                                   SourceConfig config) {
        validateConfig(config);

        String tableName = config.getTableName();
        log.info("[JDBC-SOURCE] table={} url={}", tableName, config.getJdbcUrl());

        String ddl = buildCreateTableDdl(tableName, config);
        log.debug("[JDBC-SOURCE] DDL:\n{}", ddl);
        tableEnv.executeSql(ddl);

        return tableEnv.from(tableName);
    }

    private String buildCreateTableDdl(String tableName, SourceConfig config) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(tableName).append("` (\n");

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
        } else {
            sb.append("  `value` STRING\n");
        }

        sb.append(") WITH (\n");
        sb.append("  'connector' = 'jdbc',\n");
        sb.append("  'url' = '").append(buildJdbcUrl(config)).append("',\n");
        sb.append("  'table-name' = '").append(tableName).append("',\n");
        sb.append("  'driver' = '").append(detectDriver(config.getJdbcUrl())).append("',\n");
        sb.append("  'scan.fetch-size' = '").append(config.getFetchSize()).append("'");
        if (config.getJdbcUsername() != null && !config.getJdbcUsername().isEmpty()) {
            sb.append(",\n  'username' = '").append(config.getJdbcUsername()).append("'");
            sb.append(",\n  'password' = '")
              .append(config.getJdbcPassword() != null ? config.getJdbcPassword() : "")
              .append("'");
        }
        sb.append("\n)");
        return sb.toString();
    }

    private String buildJdbcUrl(SourceConfig config) {
        String url = config.getJdbcUrl();
        if ("require".equalsIgnoreCase(config.getSslMode())
                || "verify-full".equalsIgnoreCase(config.getSslMode())) {
            String sep = url.contains("?") ? "&" : "?";
            url = url + sep + "ssl=true";
            if (config.getSslCertPath() != null && !config.getSslCertPath().isEmpty()) {
                url += "&sslcert=" + config.getSslCertPath();
            }
        }
        return url;
    }

    private String detectDriver(String jdbcUrl) {
        if (jdbcUrl.startsWith("jdbc:postgresql:")) return "org.postgresql.Driver";
        if (jdbcUrl.startsWith("jdbc:mysql:"))      return "com.mysql.cj.jdbc.Driver";
        if (jdbcUrl.startsWith("jdbc:oracle:"))     return "oracle.jdbc.driver.OracleDriver";
        if (jdbcUrl.startsWith("jdbc:h2:"))         return "org.h2.Driver";
        return "org.postgresql.Driver";
    }

    private void validateConfig(SourceConfig config) {
        if (config.getJdbcUrl() == null || config.getJdbcUrl().trim().isEmpty()) {
            throw new IllegalArgumentException("[JDBC-SOURCE] jdbcUrl must be set");
        }
        if (config.getQuery() == null || config.getQuery().trim().isEmpty()) {
            throw new IllegalArgumentException("[JDBC-SOURCE] query must be set");
        }
        if (config.getTableName() == null || config.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("[JDBC-SOURCE] tableName must be set");
        }
    }
}
