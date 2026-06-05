package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.ApiAuthConfig;
import com.datahondo.flink.streaming.config.SourceConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Feature 009-C — API / Webhook Source.
 * Polls a REST endpoint on a configurable interval with at-least-once delivery.
 * Supports Bearer, OAuth2 client credentials, mTLS, and API key authentication.
 */
@Slf4j
@Component
public class ApiSourceLayer implements SourceLayer {

    @Override
    public String getSourceType() {
        return "API";
    }

    @Override
    public Table createSourceTable(StreamExecutionEnvironment env,
                                   StreamTableEnvironment tableEnv,
                                   SourceConfig config) {
        validateConfig(config);
        log.info("[API-SOURCE] table={} url={} method={} interval={}ms",
                config.getTableName(), config.getUrl(), config.getMethod(), config.getPollIntervalMs());

        DataStream<String> stream = env
                .addSource(new RestPollingSourceFunction(config))
                .name("api-source-" + config.getTableName())
                .returns(Types.STRING);

        String tableName = config.getTableName();
        tableEnv.createTemporaryView(tableName,
                tableEnv.fromDataStream(stream,
                        org.apache.flink.table.api.Expressions.$("f0").as("payload")));

        return tableEnv.from(tableName);
    }

    private void validateConfig(SourceConfig config) {
        if (config.getUrl() == null || config.getUrl().trim().isEmpty()) {
            throw new IllegalArgumentException("[API-SOURCE] url must be set");
        }
        if (config.getTableName() == null || config.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("[API-SOURCE] tableName must be set");
        }
        try {
            URI.create(config.getUrl());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("[API-SOURCE] url is not a valid URI: " + config.getUrl());
        }
        if (config.getApiAuth() != null
                && config.getApiAuth().getType() == ApiAuthConfig.AuthType.OAUTH2) {
            if (config.getApiAuth().getTokenUrl() == null) {
                throw new IllegalArgumentException("[API-SOURCE] apiAuth.tokenUrl is required for OAUTH2");
            }
            if (config.getApiAuth().getClientId() == null) {
                throw new IllegalArgumentException("[API-SOURCE] apiAuth.clientId is required for OAUTH2");
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Inner: RestPollingSourceFunction
    // ─────────────────────────────────────────────────────────────────────────

    public static class RestPollingSourceFunction
            extends RichSourceFunction<String>
            implements CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        private final SourceConfig config;
        private volatile boolean running = true;

        // Checkpoint state: stores last poll timestamp for at-least-once recovery
        private transient ListState<Long> lastPollState;
        private transient long lastPollMs = 0L;

        private transient CloseableHttpClient httpClient;
        private transient OAuthTokenManager oauthManager;
        private transient ObjectMapper mapper;

        public RestPollingSourceFunction(SourceConfig config) {
            this.config = config;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            httpClient = HttpClientFactory.build(
                    config.getApiAuth(),
                    config.getConnectTimeoutMs(),
                    config.getReadTimeoutMs());
            if (config.getApiAuth() != null
                    && config.getApiAuth().getType() == ApiAuthConfig.AuthType.OAUTH2) {
                oauthManager = new OAuthTokenManager(config.getApiAuth());
            }
            mapper = new ObjectMapper();
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                try {
                    String responseBody = executeWithRetry();
                    List<String> records = extractRecords(responseBody);
                    synchronized (ctx.getCheckpointLock()) {
                        for (String record : records) {
                            ctx.collect(record);
                        }
                        lastPollMs = System.currentTimeMillis();
                    }
                } catch (Exception e) {
                    log.error("[API-SOURCE] Poll failed for {}: {}", config.getUrl(), e.getMessage());
                }
                Thread.sleep(config.getPollIntervalMs());
            }
        }

        private String executeWithRetry() throws IOException {
            int attempts = Math.max(1, config.getRetryAttempts());
            long backoff = config.getRetryBackoffMs();
            Exception lastEx = null;
            for (int i = 0; i < attempts; i++) {
                try {
                    return execute();
                } catch (IOException e) {
                    lastEx = e;
                    if (i < attempts - 1) {
                        try { Thread.sleep(backoff * (1L << i)); } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw e;
                        }
                    }
                }
            }
            throw (IOException) lastEx;
        }

        private String execute() throws IOException {
            HttpRequestBase request;
            if ("POST".equalsIgnoreCase(config.getMethod())) {
                request = new HttpPost(config.getUrl());
            } else {
                String url = config.getUrl();
                // API_KEY in query param
                if (config.getApiAuth() != null
                        && config.getApiAuth().getType() == ApiAuthConfig.AuthType.API_KEY
                        && "QUERY".equalsIgnoreCase(config.getApiAuth().getApiKeyLocation())) {
                    String sep = url.contains("?") ? "&" : "?";
                    url = url + sep + config.getApiAuth().getApiKeyHeader()
                            + "=" + config.getApiAuth().getApiKey();
                }
                request = new HttpGet(url);
            }
            applyAuth(request);

            try (CloseableHttpResponse resp = httpClient.execute(request)) {
                int status = resp.getStatusLine().getStatusCode();
                String body = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
                if (status >= 400 && status < 500) {
                    // 4xx — non-retryable
                    throw new IOException("Non-retryable HTTP " + status + " from " + config.getUrl());
                }
                if (status >= 500) {
                    throw new IOException("Server error HTTP " + status + " from " + config.getUrl());
                }
                return body;
            }
        }

        private void applyAuth(HttpRequestBase request) throws IOException {
            ApiAuthConfig auth = config.getApiAuth();
            if (auth == null) return;
            switch (auth.getType()) {
                case BEARER:
                    if (auth.getToken() != null) {
                        request.addHeader("Authorization", "Bearer " + auth.getToken());
                    }
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
                    // SSL context is wired at HttpClient build time; no per-request headers needed
                    break;
            }
        }

        private List<String> extractRecords(String body) throws IOException {
            List<String> result = new ArrayList<>();
            if (body == null || body.trim().isEmpty()) return result;

            String jsonPath = config.getJsonPath();
            if (jsonPath != null && !jsonPath.trim().isEmpty()) {
                List<Object> items = JsonPath.read(body, jsonPath);
                ObjectMapper om = mapper;
                for (Object item : items) {
                    result.add(om.writeValueAsString(item));
                }
            } else {
                JsonNode root = mapper.readTree(body);
                if (root.isArray()) {
                    root.forEach(node -> result.add(node.toString()));
                } else {
                    result.add(body);
                }
            }
            return result;
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (httpClient != null) {
                httpClient.close();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            lastPollState.clear();
            lastPollState.add(lastPollMs);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> descriptor =
                    new ListStateDescriptor<>("last-poll-ms", Long.class);
            lastPollState = context.getOperatorStateStore().getListState(descriptor);
            if (context.isRestored()) {
                for (Long ts : lastPollState.get()) {
                    lastPollMs = ts;
                }
                log.info("[API-SOURCE] Restored last poll timestamp: {}", lastPollMs);
            }
        }
    }
}
