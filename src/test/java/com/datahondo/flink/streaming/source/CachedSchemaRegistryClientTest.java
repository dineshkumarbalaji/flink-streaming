package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.SchemaConfig;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CachedSchemaRegistryClientTest {

    @Test
    void constructor_acceptsValidConfig() {
        SchemaConfig config = new SchemaConfig();
        config.setRegistryUrl("http://schema-registry:8081");
        config.setCacheTtlMs(300_000L);
        assertDoesNotThrow(() -> new CachedSchemaRegistryClient(config));
    }

    @Test
    void getSchema_throwsIoException_whenRegistryUnreachable() {
        SchemaConfig config = new SchemaConfig();
        config.setRegistryUrl("http://localhost:1");
        config.setCacheTtlMs(300_000L);
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(config);
        assertThrows(Exception.class, () -> client.getSchema("orders-value", "latest"));
    }

    @Test
    void invalidate_doesNotThrow_forUnknownSubject() {
        SchemaConfig config = new SchemaConfig();
        config.setRegistryUrl("http://schema-registry:8081");
        config.setCacheTtlMs(300_000L);
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(config);
        assertDoesNotThrow(() -> client.invalidate("nonexistent-subject"));
    }
}
