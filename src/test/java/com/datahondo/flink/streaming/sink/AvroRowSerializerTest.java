package com.datahondo.flink.streaming.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AvroRowSerializerTest {

    private static final String SCHEMA_STR =
            "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"int\"},"
            + "{\"name\":\"name\",\"type\":\"string\"}"
            + "]}";

    private KafkaTargetLayer.AvroRowSerializer serializer;
    private Schema schema;

    @BeforeEach
    void setUp() throws Exception {
        serializer = new KafkaTargetLayer.AvroRowSerializer(SCHEMA_STR, "test-topic");
        serializer.open(new Configuration());
        schema = new Schema.Parser().parse(SCHEMA_STR);
    }

    /** Avro schema order: [id, name]. Row order: [id, name]. Should always work. */
    @Test
    void map_withFieldsInSameOrderAsAvroSchema_serializesCorrectly() throws Exception {
        Row row = Row.withNames();
        row.setField("id", 1);
        row.setField("name", "Alice");

        byte[] bytes = serializer.map(row);

        GenericRecord record = deserialize(bytes);
        assertEquals(1, record.get("id"));
        assertEquals("Alice", record.get("name").toString());
    }

    /**
     * Avro schema order: [id, name]. Row field order: [name, id] (reversed).
     * With index-based mapping this silently writes name→id field and id→name field.
     * With name-based mapping this must produce the correct output.
     */
    @Test
    void map_withFieldsInDifferentOrderThanAvroSchema_mapsValuesByName() throws Exception {
        Row row = Row.withNames();
        row.setField("name", "Bob");   // declared first — index 0
        row.setField("id", 99);        // declared second — index 1

        byte[] bytes = serializer.map(row);

        GenericRecord record = deserialize(bytes);
        // With old index-based code: record.get("id") == "Bob" (wrong)
        // With name-based fix:        record.get("id") == 99     (correct)
        assertEquals(99, record.get("id"),
                "id field must be 99, not 'Bob' — indicates index-based mapping bug");
        assertEquals("Bob", record.get("name").toString(),
                "name field must be 'Bob', not 99 — indicates index-based mapping bug");
    }

    @Test
    void map_withNullFieldValue_setsNullInRecord() throws Exception {
        Row row = Row.withNames();
        row.setField("id", 7);
        row.setField("name", null);

        // Avro string field is not nullable in SCHEMA_STR — this may throw during serialization.
        // The test validates the serializer propagates the exception rather than silently corrupting data.
        // If you need nullable fields, use union ["null","string"] in the Avro schema.
        assertThrows(Exception.class, () -> serializer.map(row));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private GenericRecord deserialize(byte[] bytes) throws Exception {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, decoder);
    }
}
