package com.datahondo.flink.streaming.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaConfigTest {

    @Test
    void hasDefinition_returnsFalse_whenDefinitionIsNull() {
        SchemaConfig config = new SchemaConfig();
        assertFalse(config.hasDefinition());
    }

    @Test
    void hasDefinition_returnsFalse_whenDefinitionIsEmpty() {
        SchemaConfig config = new SchemaConfig();
        config.setDefinition("");
        assertFalse(config.hasDefinition());
    }

    @Test
    void hasDefinition_returnsTrue_whenDefinitionIsSet() {
        SchemaConfig config = new SchemaConfig();
        config.setDefinition("{\"type\":\"object\"}");
        assertTrue(config.hasDefinition());
    }

    @Test
    void isRegistry_returnsFalse_whenTypeIsNull() {
        SchemaConfig config = new SchemaConfig();
        assertFalse(config.isRegistry());
    }

    @Test
    void isRegistry_returnsFalse_whenTypeIsJson() {
        SchemaConfig config = new SchemaConfig();
        config.setType("JSON");
        assertFalse(config.isRegistry());
    }

    @Test
    void isRegistry_returnsTrue_whenTypeIsRegistry() {
        SchemaConfig config = new SchemaConfig();
        config.setType("REGISTRY");
        assertTrue(config.isRegistry());
    }

    @Test
    void isRegistry_isCaseInsensitive() {
        SchemaConfig config = new SchemaConfig();
        config.setType("registry");
        assertTrue(config.isRegistry());
    }
}
