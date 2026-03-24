package com.datahondo.flink.streaming.config;

import lombok.Data;

@Data
public class AuthConfig {
    private String type; // SASL_SSL, SASL_PLAINTEXT, etc.
    private String mechanism; // PLAIN, SCRAM-SHA-256, etc.
    private String username;
    private String password;
    private String protocol;
}