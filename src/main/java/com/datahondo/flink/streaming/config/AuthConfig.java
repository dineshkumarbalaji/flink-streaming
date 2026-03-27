package com.datahondo.flink.streaming.config;

import lombok.Data;

@Data
public class AuthConfig {
    private String type;      // SASL_SSL, SASL_PLAINTEXT
    private String mechanism; // PLAIN, SCRAM-SHA-256
    private String username;
    private String password;
    private String protocol;

    /**
     * Custom JAAS config string. When set, overrides the auto-generated
     * JAAS config built from username/password.
     * e.g. "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";"
     */
    private String jaasConfig;

    /** Path to the truststore JKS file (required for SASL_SSL). */
    private String truststoreLocation;

    /** Password for the truststore JKS file. */
    private String truststorePassword;
}