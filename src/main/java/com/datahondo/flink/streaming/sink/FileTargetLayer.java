package com.datahondo.flink.streaming.sink;

import com.datahondo.flink.streaming.config.SchemaConfig;
import com.datahondo.flink.streaming.config.TargetConfig;
import com.datahondo.flink.streaming.source.StoragePathResolver;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Feature 010-B — File Sink (Cold zone).
 * Writes the result Table to local filesystem, ADLS Gen2, or S3 in CSV, JSON, or Parquet format.
 * Uses Flink Table API filesystem connector DDL with checkpoint-based or size-based rolling.
 */
@Slf4j
@Component
public class FileTargetLayer implements TargetLayer {

    @Override
    public String getSinkType() {
        return "FILE";
    }

    @Override
    public void sink(StreamTableEnvironment tableEnv, Table resultTable, TargetConfig config) {
        validateConfig(config);

        String normalisedPath = StoragePathResolver.normalise(config.getStoragePath());
        String format = config.getFileFormat() == null ? "CSV" : config.getFileFormat().toUpperCase();

        log.info("[FILE-SINK] format={} path={} rollOnCheckpoint={}",
                format, normalisedPath, config.isRollOnCheckpoint());

        // Use a unique sink table name to avoid DDL conflicts across jobs
        String sinkTable = "file_sink_" + Math.abs(normalisedPath.hashCode());
        String ddl = buildSinkDdl(sinkTable, config, normalisedPath, format);
        log.debug("[FILE-SINK] DDL:\n{}", ddl);
        tableEnv.executeSql(ddl);
        resultTable.executeInsert(sinkTable);
    }

    private String buildSinkDdl(String sinkTable, TargetConfig config,
                                  String path, String format) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(sinkTable).append("` (\n");

        List<SchemaConfig.SchemaField> fields = config.getSchema() != null
                ? config.getSchema().getFields() : null;
        if (fields != null && !fields.isEmpty()) {
            for (int i = 0; i < fields.size(); i++) {
                SchemaConfig.SchemaField f = fields.get(i);
                sb.append("  `").append(f.getName()).append("` ").append(f.getType());
                if (i < fields.size() - 1) sb.append(",");
                sb.append("\n");
            }
        } else {
            sb.append("  `value` STRING\n");
        }

        if (config.getPartitionBy() != null && !config.getPartitionBy().isEmpty()) {
            sb.append(") PARTITIONED BY (`").append(config.getPartitionBy()).append("`)\n");
        } else {
            sb.append(")\n");
        }

        sb.append("WITH (\n");
        sb.append("  'connector' = 'filesystem',\n");
        sb.append("  'path' = '").append(path).append("',\n");

        switch (format) {
            case "JSON":
                sb.append("  'format' = 'json'");
                break;
            case "PARQUET":
                sb.append("  'format' = 'parquet'");
                break;
            default:
                sb.append("  'format' = 'csv',\n");
                sb.append("  'csv.field-delimiter' = ','");
        }

        if (config.isRollOnCheckpoint()) {
            sb.append(",\n  'sink.rolling-policy.check-interval' = '1min'");
            sb.append(",\n  'sink.rolling-policy.rollover-interval' = '15min'");
        }
        if (config.getMaxFileSizeBytes() > 0) {
            sb.append(",\n  'sink.rolling-policy.file-size' = '")
              .append(config.getMaxFileSizeBytes()).append("b'");
        }

        sb.append("\n)");
        return sb.toString();
    }

    private void validateConfig(TargetConfig config) {
        if (config.getStoragePath() == null || config.getStoragePath().trim().isEmpty()) {
            throw new IllegalArgumentException("[FILE-SINK] storagePath must be set");
        }
        String fmt = config.getFileFormat();
        if (fmt != null && !fmt.equalsIgnoreCase("CSV")
                && !fmt.equalsIgnoreCase("JSON")
                && !fmt.equalsIgnoreCase("PARQUET")) {
            throw new IllegalArgumentException(
                    "[FILE-SINK] fileFormat must be CSV, JSON, or PARQUET — got: " + fmt);
        }
    }
}
