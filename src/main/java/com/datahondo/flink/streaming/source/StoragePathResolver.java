package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.StorageConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Detects the URI scheme of a storage path and injects the appropriate
 * Hadoop-compatible filesystem credentials into the Flink environment.
 *
 * <p>Scheme mapping:
 * <ul>
 *   <li>{@code file:///} or bare path  → local filesystem, no credentials</li>
 *   <li>{@code abfs://}               → Azure ADLS Gen2</li>
 *   <li>{@code s3://} or {@code s3a://} → AWS S3</li>
 * </ul>
 */
@Slf4j
public final class StoragePathResolver {

    public enum StorageScheme { LOCAL, ADLS, S3 }

    private StoragePathResolver() {}

    public static StorageScheme detect(String path) {
        if (path == null || path.isEmpty()) return StorageScheme.LOCAL;
        if (path.startsWith("abfs://") || path.startsWith("abfss://")) return StorageScheme.ADLS;
        if (path.startsWith("s3://") || path.startsWith("s3a://")) return StorageScheme.S3;
        return StorageScheme.LOCAL;
    }

    /**
     * Configures the Flink environment with the necessary Hadoop FS properties
     * for the detected storage scheme.
     *
     * <p>Note: ADLS and S3 filesystem plugins must be installed in the Flink
     * {@code plugins/} directory on all cluster nodes. The credentials set here
     * are passed via Hadoop configuration properties.
     */
    public static void configure(StreamExecutionEnvironment env,
                                  String storagePath,
                                  StorageConfig storageConfig) {
        StorageScheme scheme = detect(storagePath);
        if (scheme == StorageScheme.LOCAL || storageConfig == null) return;

        Configuration conf = new Configuration();
        if (scheme == StorageScheme.ADLS) {
            configureAdls(conf, storagePath, storageConfig.getAdls());
        } else if (scheme == StorageScheme.S3) {
            configureS3(conf, storageConfig.getS3());
        }
        env.configure(conf, StoragePathResolver.class.getClassLoader());
        log.info("Configured {} filesystem credentials for path: {}", scheme, storagePath);
    }

    private static void configureAdls(Configuration conf, String path,
                                       StorageConfig.AdlsConfig adls) {
        if (adls == null) return;
        String account = adls.getAccountName();
        if (account == null || account.isEmpty()) {
            log.warn("ADLS accountName not set — filesystem may fail");
            return;
        }
        if (adls.getAccountKey() != null && !adls.getAccountKey().isEmpty()) {
            conf.setString("fs.azure.account.auth.type." + account + ".dfs.core.windows.net",
                    "SharedKey");
            conf.setString("fs.azure.account.key." + account + ".dfs.core.windows.net",
                    adls.getAccountKey());
        } else if (adls.getServicePrincipal() != null) {
            StorageConfig.ServicePrincipalConfig sp = adls.getServicePrincipal();
            conf.setString("fs.azure.account.auth.type." + account + ".dfs.core.windows.net",
                    "OAuth");
            conf.setString("fs.azure.account.oauth.provider.type." + account + ".dfs.core.windows.net",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
            conf.setString("fs.azure.account.oauth2.client.id." + account + ".dfs.core.windows.net",
                    sp.getClientId());
            conf.setString("fs.azure.account.oauth2.client.secret." + account + ".dfs.core.windows.net",
                    sp.getClientSecret());
            conf.setString("fs.azure.account.oauth2.client.endpoint." + account + ".dfs.core.windows.net",
                    "https://login.microsoftonline.com/" + sp.getTenantId() + "/oauth2/token");
        }
    }

    private static void configureS3(Configuration conf, StorageConfig.S3Config s3) {
        if (s3 == null) return;
        if (s3.getAccessKey() != null && !s3.getAccessKey().isEmpty()) {
            conf.setString("s3.access-key", s3.getAccessKey());
            conf.setString("s3.secret-key", s3.getSecretKey());
        }
        if (s3.getRegion() != null && !s3.getRegion().isEmpty()) {
            conf.setString("s3.region", s3.getRegion());
        }
        if (s3.getEndpoint() != null && !s3.getEndpoint().isEmpty()) {
            conf.setString("s3.endpoint", s3.getEndpoint());
            conf.setBoolean("s3.path.style.access", s3.isPathStyleAccess());
        }
    }

    /** Normalises path to s3a:// scheme which Flink's S3 plugin uses. */
    public static String normalise(String path) {
        if (path == null) return path;
        if (path.startsWith("s3://")) return "s3a://" + path.substring(5);
        return path;
    }
}
