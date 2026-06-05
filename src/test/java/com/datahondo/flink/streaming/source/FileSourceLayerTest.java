package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.SourceConfig;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FileSourceLayerTest {

    private final FileSourceLayer layer = new FileSourceLayer();

    @Test
    void getSourceType_returnsFile() {
        assertEquals("FILE", layer.getSourceType());
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenStoragePathIsNull() {
        SourceConfig config = new SourceConfig();
        config.setTableName("orders");
        config.setType("FILE");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenStoragePathIsBlank() {
        SourceConfig config = new SourceConfig();
        config.setTableName("orders");
        config.setStoragePath("   ");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenTableNameIsNull() {
        SourceConfig config = new SourceConfig();
        config.setStoragePath("/app/data/file.csv");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenFileFormatIsInvalid() {
        SourceConfig config = new SourceConfig();
        config.setStoragePath("/app/data/file.xml");
        config.setTableName("orders");
        config.setFileFormat("XML");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }
}
