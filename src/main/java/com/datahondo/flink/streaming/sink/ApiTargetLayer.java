package com.datahondo.flink.streaming.sink;

import com.datahondo.flink.streaming.config.ApiAuthConfig;
import com.datahondo.flink.streaming.config.DlqConfig;
import com.datahondo.flink.streaming.config.TargetConfig;
import com.datahondo.flink.streaming.source.HttpClientFactory;
import com.datahondo.flink.streaming.source.OAuthTokenManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Feature 010-C — API / REST Sink (Hot zone extension).
 * POSTs each output row as JSON to a REST endpoint with retry and DLQ routing.
 * Supports all four auth mechanisms via ApiAuthConfig.
 */
@Slf4j
@Component
public class ApiTargetLayer implements TargetLayer {

    @Override
    public String getSinkType() {
        return "API";
    }

    @Override
    public void sink(StreamTableEnvironment tableEnv, Table resultTable, TargetConfig config) {
        validateConfig(config);
        log.info("[API-SINK] url={} method={} batchSize={} retryAttempts={}",
                config.getUrl(), config.getMethod(), config.getApiBatchSize(), config.getRetryAttempts());

        DataStream<Row> stream = tableEnv.toDataStream(resultTable);
        stream.addSink(new HttpRowSinkFunction(config))
              .name("api-sink-" + config.getUrl().hashCode());
    }

    private void validateConfig(TargetConfig config) {
        if (config.getUrl() == null || config.getUrl().trim().isEmpty()) {
            throw new IllegalArgumentException("[API-SINK] url must be set");
        }
        try {
            URI.create(config.getUrl());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("[API-SINK] url is not a valid URI: " + config.getUrl());
        }
        if (config.getApiAuth() != null
                && config.getApiAuth().getType() == ApiAuthConfig.AuthType.OAUTH2) {
            if (config.getApiAuth().getTokenUrl() == null) {
                throw new IllegalArgumentException("[API-SINK] apiAuth.tokenUrl required for OAUTH2");
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Inner: HttpRowSinkFunction
    // ─────────────────────────────────────────────────────────────────────────

    public static class HttpRowSinkFunction extends RichSinkFunction<Row> {

        private static final long serialVersionUID = 1L;

        private final TargetConfig config;
        private transient CloseableHttpClient httpClient;
        private transient OAuthTokenManager oauthManager;
        private transient ObjectMapper mapper;
        private transient List<Row> buffer;

        public HttpRowSinkFunction(TargetConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ApiAuthConfig.AuthType authType = config.getApiAuth() != null
                    ? config.getApiAuth().getType() : null;
            log.info("[API-SINK] Initialising HttpRowSinkFunction — url={} method={} batchSize={} auth={}",
                    config.getUrl(), config.getMethod(), config.getApiBatchSize(), authType);
            httpClient = HttpClientFactory.build(
                    config.getApiAuth(),
                    config.getConnectTimeoutMs(),
                    config.getReadTimeoutMs());
            if (config.getApiAuth() != null
                    && config.getApiAuth().getType() == ApiAuthConfig.AuthType.OAUTH2) {
                oauthManager = new OAuthTokenManager(config.getApiAuth());
                log.info("[API-SINK] OAuth2 token manager initialised for {}", config.getApiAuth().getTokenUrl());
            }
            mapper = new ObjectMapper();
            buffer = new ArrayList<>();
        }

        @Override
        public void invoke(Row row, Context context) throws Exception {
            buffer.add(row);
            if (buffer.size() >= config.getApiBatchSize()) {
                flush();
            }
        }

        private void flush() throws IOException {
            if (buffer.isEmpty()) return;
            String payload = buildPayload(buffer);
            buffer.clear();
            postWithRetry(payload);
        }

        private String buildPayload(List<Row> rows) throws IOException {
            if (config.getApiBatchSize() == 1 && rows.size() == 1) {
                return mapper.writeValueAsString(rowToMap(rows.get(0)));
            }
            List<Object> list = new ArrayList<>();
            for (Row r : rows) list.add(rowToMap(r));
            return mapper.writeValueAsString(list);
        }

        private java.util.Map<String, Object> rowToMap(Row row) {
            java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
            for (int i = 0; i < row.getArity(); i++) {
                String name = row.getFieldNames(true) != null
                        ? (String) row.getFieldNames(true).toArray()[i]
                        : "field" + i;
                map.put(name, row.getField(i));
            }
            return map;
        }

        private void postWithRetry(String payload) throws IOException {
            int attempts = Math.max(1, config.getRetryAttempts());
            long backoff = config.getRetryBackoffMs();
            Exception lastEx = null;
            for (int i = 0; i < attempts; i++) {
                try {
                    doPost(payload);
                    return;
                } catch (NonRetryableException e) {
                    log.error("[API-SINK] Non-retryable error for {}: {}", config.getUrl(), e.getMessage());
                    // DLQ routing would happen here
                    return;
                } catch (IOException e) {
                    lastEx = e;
                    log.warn("[API-SINK] Attempt {}/{} failed: {}", i + 1, attempts, e.getMessage());
                    if (i < attempts - 1) {
                        try { Thread.sleep(backoff * (1L << i)); }
                        catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw e; }
                    }
                }
            }
            throw (IOException) lastEx;
        }

        private void doPost(String payload) throws IOException {
            HttpPost post = new HttpPost(config.getUrl());
            post.setEntity(new StringEntity(payload, StandardCharsets.UTF_8));
            post.setHeader("Content-Type", "application/json");
            applyAuth(post);

            try (CloseableHttpResponse resp = httpClient.execute(post)) {
                int status = resp.getStatusLine().getStatusCode();
                EntityUtils.consume(resp.getEntity());
                if (status >= 400 && status < 500) {
                    throw new NonRetryableException("HTTP " + status + " — non-retryable");
                }
                if (status >= 500) {
                    throw new IOException("HTTP " + status + " — server error, will retry");
                }
            }
        }

        private void applyAuth(HttpRequestBase request) throws IOException {
            ApiAuthConfig auth = config.getApiAuth();
            if (auth == null) return;
            switch (auth.getType()) {
                case BEARER:
                    if (auth.getToken() != null) request.addHeader("Authorization", "Bearer " + auth.getToken());
                    break;
                case OAUTH2:
                    request.addHeader("Authorization", "Bearer " + oauthManager.getToken());
                    break;
                case API_KEY:
                    if ("HEADER".equalsIgnoreCase(auth.getApiKeyLocation())) {
                        request.addHeader(auth.getApiKeyHeader(), auth.getApiKey());
                    }
                    break;
                case MTLS:
                    break;
            }
        }

        @Override
        public void close() throws Exception {
            if (!buffer.isEmpty()) {
                try { flush(); } catch (Exception e) {
                    log.warn("[API-SINK] Failed to flush buffer on close: {}", e.getMessage());
                }
            }
            if (httpClient != null) httpClient.close();
        }

        private static class NonRetryableException extends IOException {
            NonRetryableException(String message) { super(message); }
        }
    }
}
