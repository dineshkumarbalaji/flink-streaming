package com.datahondo.flink.streaming.savepoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Production implementation of {@link FlinkRestClient} using plain Java
 * {@link HttpURLConnection} — compatible with Java 8 and requires no extra dependencies.
 *
 * <p>Flink REST API reference (v1.18):
 * <ul>
 *   <li>POST {@code /v1/jobs/{jobId}/savepoints} — trigger savepoint</li>
 *   <li>GET  {@code /v1/jobs/{jobId}/savepoints/{requestId}} — poll status</li>
 * </ul>
 */
@Slf4j
@Component
public class HttpFlinkRestClient implements FlinkRestClient {

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS    = 30_000;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String postSavepointRequest(String host, int port, String jobId,
                                       String targetDir, boolean cancelJob) throws Exception {
        String urlStr = String.format("http://%s:%d/v1/jobs/%s/savepoints", host, port, jobId);
        String body = String.format(
                "{\"target-directory\":\"%s\",\"cancel-job\":%b}", targetDir, cancelJob);

        log.info("[SAVEPOINT] POST {} — target={} cancel={}", urlStr, targetDir, cancelJob);
        String response = httpPost(urlStr, body);
        JsonNode node = mapper.readTree(response);
        String requestId = node.get("request-id").asText();
        log.info("[SAVEPOINT] Request accepted — requestId={}", requestId);
        return requestId;
    }

    @Override
    public SavepointStatusResponse getSavepointStatus(String host, int port,
                                                      String jobId, String requestId) throws Exception {
        String urlStr = String.format(
                "http://%s:%d/v1/jobs/%s/savepoints/%s", host, port, jobId, requestId);
        String response = httpGet(urlStr);
        JsonNode node = mapper.readTree(response);

        String status = node.path("status").path("id").asText("UNKNOWN");
        String location = null;
        if ("COMPLETED".equals(status)) {
            location = node.path("operation").path("location").asText(null);
        }
        log.debug("[SAVEPOINT] Status poll — requestId={} status={}", requestId, status);
        return new SavepointStatusResponse(status, location);
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────

    private String httpPost(String urlStr, String body) throws Exception {
        HttpURLConnection conn = openConnection(urlStr, "POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return readBody(conn);
    }

    private String httpGet(String urlStr) throws Exception {
        HttpURLConnection conn = openConnection(urlStr, "GET");
        return readBody(conn);
    }

    private HttpURLConnection openConnection(String urlStr, String method) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        return conn;
    }

    private String readBody(HttpURLConnection conn) throws Exception {
        int code = conn.getResponseCode();
        InputStream is = (code < 400) ? conn.getInputStream() : conn.getErrorStream();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            String body = sb.toString();
            if (code >= 400) {
                throw new SavepointException(
                        "Flink REST API returned HTTP " + code + ": " + body);
            }
            return body;
        }
    }
}
