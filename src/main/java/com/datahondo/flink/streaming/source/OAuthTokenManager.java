package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.ApiAuthConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages an OAuth2 client credentials access token.
 * Fetches a new token on first use and refreshes it 60 seconds before expiry.
 * Thread-safe via synchronized access to token state.
 */
@Slf4j
public class OAuthTokenManager implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final long REFRESH_BUFFER_MS = 60_000L;

    private final ApiAuthConfig auth;
    private final ObjectMapper mapper = new ObjectMapper();

    private transient volatile String accessToken;
    private transient volatile long expiresAtMs = 0;

    public OAuthTokenManager(ApiAuthConfig auth) {
        this.auth = auth;
    }

    public synchronized String getToken() throws IOException {
        if (accessToken == null || System.currentTimeMillis() >= expiresAtMs - REFRESH_BUFFER_MS) {
            refresh();
        }
        return accessToken;
    }

    private void refresh() throws IOException {
        log.debug("Fetching OAuth2 token from {}", auth.getTokenUrl());
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(auth.getTokenUrl());
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("grant_type", "client_credentials"));
            params.add(new BasicNameValuePair("client_id", auth.getClientId()));
            params.add(new BasicNameValuePair("client_secret", auth.getClientSecret()));
            if (auth.getScope() != null && !auth.getScope().isEmpty()) {
                params.add(new BasicNameValuePair("scope", auth.getScope()));
            }
            post.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));

            try (CloseableHttpResponse resp = client.execute(post)) {
                int status = resp.getStatusLine().getStatusCode();
                String body = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
                if (status != 200) {
                    throw new IOException("OAuth2 token fetch failed — HTTP " + status + ": " + body);
                }
                JsonNode node = mapper.readTree(body);
                accessToken = node.path("access_token").asText();
                long expiresIn = node.path("expires_in").asLong(3600L);
                expiresAtMs = System.currentTimeMillis() + expiresIn * 1000L;
                log.info("OAuth2 token refreshed, expires in {}s", expiresIn);
            }
        }
    }
}
