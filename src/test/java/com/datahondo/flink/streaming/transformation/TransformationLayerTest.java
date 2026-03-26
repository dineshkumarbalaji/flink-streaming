package com.datahondo.flink.streaming.transformation;

import com.datahondo.flink.streaming.config.TransformationConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class TransformationLayerTest {

    @Mock
    private StreamTableEnvironment tableEnv;

    @Mock
    private Table resultTable;

    private TransformationLayer layer;

    @BeforeEach
    void setUp() {
        layer = new TransformationLayer();
        when(tableEnv.sqlQuery(anyString())).thenReturn(resultTable);
    }

    @Test
    void applyTransformation_usesInlineSql_whenTypeIsInline() throws Exception {
        TransformationConfig config = new TransformationConfig();
        config.setType("INLINE");
        config.setSqlContent("SELECT * FROM orders");
        config.setResultTableName("result");

        Table result = layer.applyTransformation(tableEnv, config);

        verify(tableEnv).sqlQuery("SELECT * FROM orders");
        verify(tableEnv).createTemporaryView("result", resultTable);
        assertSame(resultTable, result);
    }

    @Test
    void applyTransformation_usesInlineSql_whenTypeIsNull() throws Exception {
        TransformationConfig config = new TransformationConfig();
        config.setType(null);
        config.setSqlContent("SELECT 1");
        config.setResultTableName("r");

        layer.applyTransformation(tableEnv, config);

        verify(tableEnv).sqlQuery("SELECT 1");
    }

    @Test
    void applyTransformation_throws_whenTypeIsInlineButSqlEmpty() {
        TransformationConfig config = new TransformationConfig();
        config.setType("INLINE");
        config.setSqlContent("");
        config.setResultTableName("r");

        assertThrows(Exception.class, () -> layer.applyTransformation(tableEnv, config));
    }

    @Test
    void applyTransformation_throws_whenTypeIsFileButPathMissing() {
        TransformationConfig config = new TransformationConfig();
        config.setType("FILE");
        config.setSqlFilePath(null);
        config.setResultTableName("r");

        assertThrows(IllegalArgumentException.class, () -> layer.applyTransformation(tableEnv, config));
    }

    @Test
    void applyTransformation_throws_whenTypeIsUnknown() {
        TransformationConfig config = new TransformationConfig();
        config.setType("GRAPHQL");
        config.setResultTableName("r");

        assertThrows(IllegalArgumentException.class, () -> layer.applyTransformation(tableEnv, config));
    }

    @Test
    void applyTransformation_usesDefaultResultTableName() throws Exception {
        TransformationConfig config = new TransformationConfig();
        config.setType("INLINE");
        config.setSqlContent("SELECT 1");
        config.setResultTableName("my_result");

        layer.applyTransformation(tableEnv, config);

        verify(tableEnv).createTemporaryView(eq("my_result"), any(Table.class));
    }
}
