package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.SourceConfig;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JdbcSourceLayerTest {

    private final JdbcSourceLayer layer = new JdbcSourceLayer();

    @Test
    void getSourceType_returnsJdbc() {
        assertEquals("JDBC", layer.getSourceType());
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenJdbcUrlIsNull() {
        SourceConfig config = new SourceConfig();
        config.setTableName("customers");
        config.setQuery("SELECT * FROM customers");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenQueryIsNull() {
        SourceConfig config = new SourceConfig();
        config.setTableName("customers");
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/db");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenTableNameIsNull() {
        SourceConfig config = new SourceConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/db");
        config.setQuery("SELECT * FROM customers");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }
}
