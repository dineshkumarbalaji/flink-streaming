package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.ApiAuthConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;

/**
 * Builds an Apache HttpClient 4.x instance wired with the correct SSL context
 * for the configured auth type. Used by both API source and API sink.
 */
@Slf4j
public final class HttpClientFactory {

    private HttpClientFactory() {}

    public static CloseableHttpClient build(ApiAuthConfig auth, int connectMs, int readMs) {
        if (auth != null && auth.getType() == ApiAuthConfig.AuthType.MTLS) {
            return buildMtlsClient(auth, connectMs, readMs);
        }
        return HttpClients.custom()
                .setDefaultRequestConfig(
                        org.apache.http.client.config.RequestConfig.custom()
                                .setConnectTimeout(connectMs)
                                .setSocketTimeout(readMs)
                                .build())
                .build();
    }

    private static CloseableHttpClient buildMtlsClient(ApiAuthConfig auth,
                                                         int connectMs, int readMs) {
        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            char[] ksPwd = auth.getKeystorePassword() != null
                    ? auth.getKeystorePassword().toCharArray() : new char[0];
            try (FileInputStream fis = new FileInputStream(auth.getKeystorePath())) {
                keyStore.load(fis, ksPwd);
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, ksPwd);

            SSLContextBuilder ctxBuilder = SSLContextBuilder.create()
                    .loadKeyMaterial(keyStore, ksPwd);

            if (auth.getTruststorePath() != null && !auth.getTruststorePath().isEmpty()) {
                KeyStore trustStore = KeyStore.getInstance("JKS");
                char[] tsPwd = auth.getTruststorePassword() != null
                        ? auth.getTruststorePassword().toCharArray() : new char[0];
                try (FileInputStream fis = new FileInputStream(auth.getTruststorePath())) {
                    trustStore.load(fis, tsPwd);
                }
                ctxBuilder.loadTrustMaterial(trustStore, null);
            } else {
                ctxBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            }

            SSLContext sslContext = ctxBuilder.build();
            SSLConnectionSocketFactory sslSf = new SSLConnectionSocketFactory(
                    sslContext, NoopHostnameVerifier.INSTANCE);

            log.info("Built mTLS HttpClient using keystore: {}", auth.getKeystorePath());
            return HttpClients.custom()
                    .setSSLSocketFactory(sslSf)
                    .setDefaultRequestConfig(
                            org.apache.http.client.config.RequestConfig.custom()
                                    .setConnectTimeout(connectMs)
                                    .setSocketTimeout(readMs)
                                    .build())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build mTLS HttpClient: " + e.getMessage(), e);
        }
    }
}
