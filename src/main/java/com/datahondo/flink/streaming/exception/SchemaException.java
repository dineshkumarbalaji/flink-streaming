package com.datahondo.flink.streaming.exception;

public class SchemaException extends RuntimeException {
    public SchemaException(String message) {
        super(message);
    }

    public SchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
