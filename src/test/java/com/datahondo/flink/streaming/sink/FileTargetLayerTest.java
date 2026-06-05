package com.datahondo.flink.streaming.sink;

import com.datahondo.flink.streaming.config.TargetConfig;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FileTargetLayerTest {

    private final FileTargetLayer layer = new FileTargetLayer();

    @Test
    void getSinkType_returnsFile() {
        assertEquals("FILE", layer.getSinkType());
    }

    @Test
    void sink_throwsIllegalArgument_whenStoragePathIsNull() {
        TargetConfig config = new TargetConfig();
        assertThrows(IllegalArgumentException.class,
                () -> layer.sink(null, null, config));
    }

    @Test
    void sink_throwsIllegalArgument_whenStoragePathIsBlank() {
        TargetConfig config = new TargetConfig();
        config.setStoragePath("   ");
        assertThrows(IllegalArgumentException.class,
                () -> layer.sink(null, null, config));
    }

    @Test
    void sink_throwsIllegalArgument_whenFileFormatIsInvalid() {
        TargetConfig config = new TargetConfig();
        config.setStoragePath("/app/output");
        config.setFileFormat("XML");
        assertThrows(IllegalArgumentException.class,
                () -> layer.sink(null, null, config));
    }
}
