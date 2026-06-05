package com.datahondo.flink.streaming.sink;

import java.util.List;

/**
 * Generates dialect-specific SQL for plain INSERT and upsert operations.
 */
public enum JdbcDialect {

    POSTGRESQL {
        @Override
        public String upsertSql(String table, List<String> columns, List<String> keyColumns) {
            String cols = String.join(", ", columns);
            String placeholders = buildPlaceholders(columns.size());
            String updates = buildUpdateSet(columns, keyColumns);
            String keys = String.join(", ", keyColumns);
            return "INSERT INTO " + table + " (" + cols + ") VALUES (" + placeholders + ")"
                    + " ON CONFLICT (" + keys + ") DO UPDATE SET " + updates;
        }
    },

    MYSQL {
        @Override
        public String upsertSql(String table, List<String> columns, List<String> keyColumns) {
            String cols = String.join(", ", columns);
            String placeholders = buildPlaceholders(columns.size());
            StringBuilder updates = new StringBuilder();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) updates.append(", ");
                updates.append(columns.get(i)).append("=VALUES(").append(columns.get(i)).append(")");
            }
            return "INSERT INTO " + table + " (" + cols + ") VALUES (" + placeholders + ")"
                    + " ON DUPLICATE KEY UPDATE " + updates;
        }
    },

    ORACLE {
        @Override
        public String upsertSql(String table, List<String> columns, List<String> keyColumns) {
            StringBuilder sb = new StringBuilder("MERGE INTO ").append(table).append(" t USING dual ON (");
            for (int i = 0; i < keyColumns.size(); i++) {
                if (i > 0) sb.append(" AND ");
                sb.append("t.").append(keyColumns.get(i)).append("=?");
            }
            sb.append(") WHEN MATCHED THEN UPDATE SET ");
            boolean first = true;
            for (String col : columns) {
                if (!keyColumns.contains(col)) {
                    if (!first) sb.append(", ");
                    sb.append("t.").append(col).append("=?");
                    first = false;
                }
            }
            sb.append(" WHEN NOT MATCHED THEN INSERT (").append(String.join(", ", columns))
              .append(") VALUES (").append(buildPlaceholders(columns.size())).append(")");
            return sb.toString();
        }
    },

    H2 {
        @Override
        public String upsertSql(String table, List<String> columns, List<String> keyColumns) {
            // H2 MERGE INTO syntax
            String cols = String.join(", ", columns);
            String placeholders = buildPlaceholders(columns.size());
            String keys = String.join(", ", keyColumns);
            return "MERGE INTO " + table + " (" + cols + ") KEY (" + keys + ")"
                    + " VALUES (" + placeholders + ")";
        }
    };

    public abstract String upsertSql(String table, List<String> columns, List<String> keyColumns);

    public String insertSql(String table, List<String> columns) {
        return "INSERT INTO " + table + " (" + String.join(", ", columns) + ")"
                + " VALUES (" + buildPlaceholders(columns.size()) + ")";
    }

    protected static String buildPlaceholders(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) sb.append(", ");
            sb.append("?");
        }
        return sb.toString();
    }

    protected static String buildUpdateSet(List<String> columns, List<String> keyColumns) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String col : columns) {
            if (!keyColumns.contains(col)) {
                if (!first) sb.append(", ");
                sb.append(col).append("=EXCLUDED.").append(col);
                first = false;
            }
        }
        return sb.toString();
    }

    /** Auto-detects dialect from JDBC URL. */
    public static JdbcDialect detect(String jdbcUrl) {
        if (jdbcUrl == null) return POSTGRESQL;
        if (jdbcUrl.startsWith("jdbc:postgresql:")) return POSTGRESQL;
        if (jdbcUrl.startsWith("jdbc:mysql:"))      return MYSQL;
        if (jdbcUrl.startsWith("jdbc:oracle:"))     return ORACLE;
        if (jdbcUrl.startsWith("jdbc:h2:"))         return H2;
        return POSTGRESQL;
    }
}
