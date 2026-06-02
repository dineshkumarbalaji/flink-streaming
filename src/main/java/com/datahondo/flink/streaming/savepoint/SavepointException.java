package com.datahondo.flink.streaming.savepoint;

/**
 * Thrown when a savepoint operation fails or times out.
 */
public class SavepointException extends Exception {

    public SavepointException(String message) {
        super(message);
    }

    public SavepointException(String message, Throwable cause) {
        super(message, cause);
    }
}
