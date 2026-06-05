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
 * Feature 009-A — File / Batch Source.
 * Reads CSV, JSON, or Parquet files from local filesystem, ADLS Gen2, or S3.
 * Uses Flink Table API filesystem connector DDL — no DataStream API imports needed.
 */
@Slf4j
@Component
public class FileSourceLayer implements SourceLayer {

    @Override
    public String getSourceType() {
        return "FILE";
    }

    @Override
    public Table createSourceTable(StreamExecutionEnvironment env,
                                   StreamTableEnvironment tableEnv,
                                   SourceConfig config) {
        validateConfig(config);

        // Configure cloud storage credentials into the Flink environment
        StoragePathResolver.configure(env, config.getStoragePath(), config.getStorage());
        String normalisedPath = StoragePathResolver.normalise(config.getStoragePath());

        String format = config.getFileFormat() == null ? "CSV" : config.getFileFormat().toUpperCase();
        String tableName = config.getTableName();

        log.info("[FILE-SOURCE] table={} format={} path={}", tableName, format, normalisedPath);

        String ddl = buildCreateTableDdl(tableName, config, normalisedPath, format);
        log.debug("[FILE-SOURCE] DDL:\n{}", ddl);
        tableEnv.executeSql(ddl);

        return tableEnv.from(tableName);
    }

    private String buildCreateTableDdl(String tableName, SourceConfig config,
                                        String path, String format) {
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
        sb.append("  'connector' = 'filesystem',\n");
        sb.append("  'path' = '").append(path).append("',\n");

        switch (format) {
            case "JSON":
                sb.append("  'format' = 'json'\n");
                break;
            case "PARQUET":
                sb.append("  'format' = 'parquet'\n");
                break;
            default:
                sb.append("  'format' = 'csv',\n");
                sb.append("  'csv.ignore-parse-errors' = 'true'\n");
        }

        sb.append(")");
        return sb.toString();
    }

    private void validateConfig(SourceConfig config) {
        if (config.getStoragePath() == null || config.getStoragePath().trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "[FILE-SOURCE] storagePath must be set for FILE source type");
        }
        if (config.getTableName() == null || config.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("[FILE-SOURCE] tableName must be set");
        }
        String fmt = config.getFileFormat();
        if (fmt != null && !fmt.equalsIgnoreCase("CSV")
                && !fmt.equalsIgnoreCase("JSON")
                && !fmt.equalsIgnoreCase("PARQUET")) {
            throw new IllegalArgumentException(
                    "[FILE-SOURCE] fileFormat must be CSV, JSON, or PARQUET — got: " + fmt);
        }
    }
}
