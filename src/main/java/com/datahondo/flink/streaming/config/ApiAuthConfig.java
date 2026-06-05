package com.datahondo.flink.streaming.config;

import lombok.Data;

/**
 * Authentication configuration for API source and sink layers.
 * Supports Bearer token, OAuth2 client credentials, mTLS, and API key.
 */
@Data
public class ApiAuthConfig {

    public enum AuthType { BEARER, OAUTH2, MTLS, API_KEY }

    /** Discriminator — selects which auth fields to use. */
    private AuthType type = AuthType.BEARER;

    // ── BEARER ──────────────────────────────────────────────────────────────
    /** Static bearer token sent as Authorization: Bearer <token>. */
    private String token;

    // ── OAUTH2 ───────────────────────────────────────────────────────────────
    /** Token endpoint URL for OAuth2 client credentials flow. */
    private String tokenUrl;
    private String clientId;
    private String clientSecret;
    /** Space-separated scope string; may be null. */
    private String scope;

    // ── MTLS ─────────────────────────────────────────────────────────────────
    /** Path to PKCS#12 or JKS keystore containing the client certificate. */
    private String keystorePath;
    private String keystorePassword;
    /** Path to JKS truststore containing the server CA certificate. */
    private String truststorePath;
    private String truststorePassword;

    // ── API_KEY ───────────────────────────────────────────────────────────────
    private String apiKey;
    /** Header name for the API key; defaults to X-Api-Key. */
    private String apiKeyHeader = "X-Api-Key";
    /** Where to send the key — HEADER or QUERY. */
    private String apiKeyLocation = "HEADER";
}
