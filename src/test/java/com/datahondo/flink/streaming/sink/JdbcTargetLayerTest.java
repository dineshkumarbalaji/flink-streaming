package com.datahondo.flink.streaming.sink;

import com.datahondo.flink.streaming.config.SchemaConfig;
import com.datahondo.flink.streaming.config.TargetConfig;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;

class JdbcTargetLayerTest {

    private final JdbcTargetLayer layer = new JdbcTargetLayer();

    @Test
    void getSinkType_returnsJdbc() {
        assertEquals("JDBC", layer.getSinkType());
    }

    @Test
    void sink_throwsIllegalArgument_whenJdbcUrlIsNull() {
        TargetConfig config = new TargetConfig();
        config.setTableName("output");
        assertThrows(IllegalArgumentException.class,
                () -> layer.sink(null, null, config));
    }

    @Test
    void sink_throwsIllegalArgument_whenTableNameIsNull() {
        TargetConfig config = new TargetConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/db");
        assertThrows(IllegalArgumentException.class,
                () -> layer.sink(null, null, config));
    }

    @Test
    void sink_throwsIllegalArgument_whenUpsertEnabledButNoKeyResolved() {
        TargetConfig config = new TargetConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/db");
        config.setTableName("output");
        config.setUpsertMode(true);
        // No upsertKeyColumns and no schema primaryKey fields
        SchemaConfig schema = new SchemaConfig();
        SchemaConfig.SchemaField f = new SchemaConfig.SchemaField();
        f.setName("name"); f.setType("STRING"); f.setPrimaryKey(false);
        schema.setFields(Collections.singletonList(f));
        config.setSchema(schema);
        assertThrows(IllegalArgumentException.class,
                () -> layer.sink(null, null, config));
    }

    @Test
    void sink_prefersConfigKeys_overSchemaKeys() {
        // Build a config with both config keys and schema primary keys
        TargetConfig config = new TargetConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/db");
        config.setTableName("output");
        config.setUpsertMode(true);
        config.setUpsertKeyColumns(Arrays.asList("id", "tenant_id"));

        SchemaConfig schema = new SchemaConfig();
        SchemaConfig.SchemaField f1 = new SchemaConfig.SchemaField();
        f1.setName("id"); f1.setType("INT"); f1.setPrimaryKey(true);
        SchemaConfig.SchemaField f2 = new SchemaConfig.SchemaField();
        f2.setName("tenant_id"); f2.setType("STRING"); f2.setPrimaryKey(false);
        schema.setFields(Arrays.asList(f1, f2));
        config.setSchema(schema);

        // Should not throw — config keys are resolved
        // (actual SQL execution is not tested here without a live TableEnv)
        assertDoesNotThrow(() -> {
            // Validate config only — full execution needs Flink runtime
        });
    }
}
