package com.datahondo.flink.streaming.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaSourceLayer's JSON and Avro schema parsing methods.
 * Uses reflection to access package-private helpers.
 */
class SchemaParserTest {

    private KafkaSourceLayer layer;
    private Method parseJsonSchema;
    private Method parseAvroSchema;

    @BeforeEach
    void setUp() throws Exception {
        layer = new KafkaSourceLayer();
        parseJsonSchema = KafkaSourceLayer.class.getDeclaredMethod("parseSchemaString", String.class);
        parseJsonSchema.setAccessible(true);
        parseAvroSchema = KafkaSourceLayer.class.getDeclaredMethod("parseAvroSchemaString", String.class);
        parseAvroSchema.setAccessible(true);
    }

    // ── JSON Schema ────────────────────────────────────────────────────────────

    @Test

    void parseJsonSchema_mapsStringType() throws Exception {
        String schema = "{\"properties\":{\"name\":{\"type\":\"string\"}}}";
        List<?> fields = (List<?>) parseJsonSchema.invoke(layer, schema);
        assertEquals(1, fields.size());
        assertEquals("STRING", fieldType(fields.get(0)));
        assertEquals("name", fieldName(fields.get(0)));
    }

    @Test

    void parseJsonSchema_mapsIntegerType() throws Exception {
        String schema = "{\"properties\":{\"age\":{\"type\":\"integer\"}}}";
        List<?> fields = (List<?>) parseJsonSchema.invoke(layer, schema);
        assertEquals("INT", fieldType(fields.get(0)));
    }

    @Test

    void parseJsonSchema_mapsNumberToDouble() throws Exception {
        String schema = "{\"properties\":{\"score\":{\"type\":\"number\"}}}";
        List<?> fields = (List<?>) parseJsonSchema.invoke(layer, schema);
        assertEquals("DOUBLE", fieldType(fields.get(0)));
    }

    @Test

    void parseJsonSchema_mapsBooleanType() throws Exception {
        String schema = "{\"properties\":{\"active\":{\"type\":\"boolean\"}}}";
        List<?> fields = (List<?>) parseJsonSchema.invoke(layer, schema);
        assertEquals("BOOLEAN", fieldType(fields.get(0)));
    }

    @Test

    void parseJsonSchema_unknownTypeDefaultsToString() throws Exception {
        String schema = "{\"properties\":{\"blob\":{\"type\":\"array\"}}}";
        List<?> fields = (List<?>) parseJsonSchema.invoke(layer, schema);
        assertEquals("STRING", fieldType(fields.get(0)));
    }

    @Test
    void parseJsonSchema_throwsSchemaException_onMalformedJson() {
        assertThrows(Exception.class, () -> parseJsonSchema.invoke(layer, "not-json"));
    }

    // ── Avro Schema ───────────────────────────────────────────────────────────

    @Test

    void parseAvroSchema_mapsLongToBigint() throws Exception {
        String schema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"ts\",\"type\":\"long\"}]}";
        List<?> fields = (List<?>) parseAvroSchema.invoke(layer, schema);
        assertEquals("LONG", fieldType(fields.get(0)));
    }

    @Test

    void parseAvroSchema_mapsIntToInt() throws Exception {
        String schema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"qty\",\"type\":\"int\"}]}";
        List<?> fields = (List<?>) parseAvroSchema.invoke(layer, schema);
        assertEquals("INT", fieldType(fields.get(0)));
    }

    @Test

    void parseAvroSchema_mapsNullableUnion() throws Exception {
        String schema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"val\",\"type\":[\"null\",\"string\"]}]}";
        List<?> fields = (List<?>) parseAvroSchema.invoke(layer, schema);
        assertEquals("STRING", fieldType(fields.get(0)));
    }

    @Test
    void parseAvroSchema_throwsSchemaException_onMalformedAvro() {
        assertThrows(Exception.class, () -> parseAvroSchema.invoke(layer, "{}"));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private String fieldName(Object field) throws Exception {
        return (String) field.getClass().getDeclaredField("name").get(field);
    }

    private String fieldType(Object field) throws Exception {
        return (String) field.getClass().getDeclaredField("type").get(field);
    }
}
