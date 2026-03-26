package com.datahondo.flink.streaming.source;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for KafkaSourceLayer.SchemaValidator flatMap logic.
 * Exercises valid records, schema violations, type coercion, and null fields.
 */
class SchemaValidatorTest {

    private List<KafkaSourceLayer.SchemaValidator> validators;

    @BeforeEach
    void setUp() throws Exception {
        // Build a two-field schema: id(INT), name(STRING)
        validators = new ArrayList<>();
        // We call the two-arg constructor with a real JSON Schema string
        String jsonSchema = "{"
            + "\"type\":\"object\","
            + "\"properties\":{"
            + "  \"id\":{\"type\":\"integer\"},"
            + "  \"name\":{\"type\":\"string\"}"
            + "},"
            + "\"required\":[\"id\",\"name\"]"
            + "}";

        // FieldDefinition is package-private; build validator via reflection helper
        KafkaSourceLayer layer = new KafkaSourceLayer();
        java.lang.reflect.Method parse = KafkaSourceLayer.class.getDeclaredMethod("parseSchemaString", String.class);
        parse.setAccessible(true);
        List<?> fields = (List<?>) parse.invoke(layer, jsonSchema);

        KafkaSourceLayer.SchemaValidator validator = new KafkaSourceLayer.SchemaValidator(
                castFields(fields), jsonSchema);
        // Inject a mock RuntimeContext so getLongCounter() works without a Flink cluster
        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        when(runtimeContext.getLongCounter(anyString())).thenReturn(new LongCounter());
        validator.setRuntimeContext(runtimeContext);
        validator.open(new Configuration());
        validators.add(validator);
    }

    @Test
    void flatMap_emitsRow_forValidRecord() throws Exception {
        List<Row> out = new ArrayList<>();
        validators.get(0).flatMap("{\"id\":1,\"name\":\"alice\"}", new ListCollector<>(out));
        assertEquals(1, out.size());
        assertEquals(1, out.get(0).getField(0));
        assertEquals("alice", out.get(0).getField(1));
    }

    @Test
    void flatMap_dropsRecord_whenRequiredFieldMissing() throws Exception {
        List<Row> out = new ArrayList<>();
        // "name" is required but absent
        validators.get(0).flatMap("{\"id\":2}", new ListCollector<>(out));
        assertEquals(0, out.size());
    }

    @Test
    void flatMap_dropsRecord_onMalformedJson() throws Exception {
        List<Row> out = new ArrayList<>();
        validators.get(0).flatMap("not-json", new ListCollector<>(out));
        assertEquals(0, out.size());
    }

    @Test
    void flatMap_setsNullField_whenOptionalFieldAbsent() throws Exception {
        // Validator without a schema string (no JSON-schema enforcement)
        KafkaSourceLayer layer = new KafkaSourceLayer();
        java.lang.reflect.Method parse = KafkaSourceLayer.class.getDeclaredMethod("parseSchemaString", String.class);
        parse.setAccessible(true);
        String relaxedSchema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"}}}";
        List<?> fields = (List<?>) parse.invoke(layer, relaxedSchema);

        KafkaSourceLayer.SchemaValidator v = new KafkaSourceLayer.SchemaValidator(castFields(fields));
        RuntimeContext ctx2 = mock(RuntimeContext.class);
        when(ctx2.getLongCounter(anyString())).thenReturn(new LongCounter());
        v.setRuntimeContext(ctx2);
        v.open(new Configuration());

        List<Row> out = new ArrayList<>();
        v.flatMap("{\"id\":5}", new ListCollector<>(out));
        assertEquals(1, out.size());
        assertNull(out.get(0).getField(1)); // name is absent → null
    }

    // Cast via unchecked — FieldDefinition is package-private
    @SuppressWarnings({"unchecked", "rawtypes"})
    private List castFields(List<?> raw) {
        return raw;
    }
}
