package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.SchemaConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Base64;

/**
 * Feature 011 — Schema Registry Client.
 * Fetches Avro schema from a SASL-secured Confluent Schema Registry REST API.
 * Uses Apache HttpClient 4.x (Java 8 compatible) with Basic auth for PLAIN/SCRAM.
 */
@Slf4j
public class SchemaRegistryClient {

    private final SchemaConfig config;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();

    public SchemaRegistryClient(SchemaConfig config) {
        this.config = config;
        this.httpClient = buildHttpClient(config);
    }

    /**
     * Fetches the Avro schema string for the given subject and version.
     *
     * @param subject schema registry subject (e.g. "orders-value")
     * @param version "latest" or a numeric version
     * @return Avro schema JSON string
     * @throws IOException if the registry is unreachable or returns a non-200 status
     */
    public String fetchSchema(String subject, String version) throws IOException {
        String url = config.getRegistryUrl().replaceAll("/$", "")
                + "/subjects/" + subject + "/versions/" + version;
        log.debug("[SCHEMA-REGISTRY] GET {}", url);

        HttpGet request = new HttpGet(url);
        request.setHeader("Accept", "application/vnd.schemaregistry.v1+json, application/json");
        applyAuth(request);

        try (CloseableHttpResponse resp = httpClient.execute(request)) {
            int status = resp.getStatusLine().getStatusCode();
            String body = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);

            if (status == 404) {
                throw new IOException("Schema not found in registry — subject=" + subject
                        + " version=" + version + " url=" + url);
            }
            if (status == 401 || status == 403) {
                throw new IOException("Schema Registry authentication failed (HTTP " + status
                        + ") — check username/password and saslMechanism");
            }
            if (status != 200) {
                throw new IOException("Schema Registry returned HTTP " + status + " — " + body);
            }

            JsonNode node = mapper.readTree(body);
            String schema = node.path("schema").asText();
            if (schema == null || schema.isEmpty()) {
                throw new IOException("Schema Registry response contained no 'schema' field: " + body);
            }
            log.info("[SCHEMA-REGISTRY] Fetched schema for subject={} version={}", subject, version);
            return schema;
        }
    }

    private void applyAuth(HttpGet request) {
        String username = config.getRegistryUsername();
        String password = config.getRegistryPassword();
        if (username != null && !username.isEmpty()) {
            String credentials = username + ":" + (password != null ? password : "");
            String encoded = Base64.getEncoder().encodeToString(
                    credentials.getBytes(StandardCharsets.UTF_8));
            request.setHeader("Authorization", "Basic " + encoded);
        }
    }

    private CloseableHttpClient buildHttpClient(SchemaConfig config) {
        RequestConfig reqConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(10000)
                .build();

        SchemaConfig.RegistryTlsConfig tls = config.getTls();
        if (tls != null && tls.isEnabled()) {
            return buildTlsClient(tls, reqConfig);
        }
        return HttpClients.custom().setDefaultRequestConfig(reqConfig).build();
    }

    private CloseableHttpClient buildTlsClient(SchemaConfig.RegistryTlsConfig tls,
                                                RequestConfig reqConfig) {
        try {
            SSLContextBuilder ctxBuilder = SSLContextBuilder.create();
            if (tls.getTruststorePath() != null && !tls.getTruststorePath().isEmpty()) {
                KeyStore trustStore = KeyStore.getInstance("JKS");
                char[] pwd = tls.getTruststorePassword() != null
                        ? tls.getTruststorePassword().toCharArray() : new char[0];
                try (FileInputStream fis = new FileInputStream(tls.getTruststorePath())) {
                    trustStore.load(fis, pwd);
                }
                ctxBuilder.loadTrustMaterial(trustStore, null);
            } else {
                ctxBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            }
            SSLContext sslContext = ctxBuilder.build();
            SSLConnectionSocketFactory sslSf = tls.isSkipHostnameVerification()
                    ? new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE)
                    : new SSLConnectionSocketFactory(sslContext);
            return HttpClients.custom()
                    .setSSLSocketFactory(sslSf)
                    .setDefaultRequestConfig(reqConfig)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build TLS HttpClient for Schema Registry: "
                    + e.getMessage(), e);
        }
    }

    public void close() throws IOException {
        httpClient.close();
    }
}
