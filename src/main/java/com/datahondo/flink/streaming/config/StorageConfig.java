package com.datahondo.flink.streaming.config;

import lombok.Data;

/**
 * Cloud storage credentials for File source and sink layers.
 * URI scheme in storagePath selects the active block:
 *   file:/// or bare path → local (no credentials needed)
 *   abfs://              → adls block
 *   s3:// or s3a://      → s3 block
 */
@Data
public class StorageConfig {

    @Data
    public static class AdlsConfig {
        private String accountName;
        /** Storage account key (base64). Mutually exclusive with servicePrincipal. */
        private String accountKey;
        private ServicePrincipalConfig servicePrincipal;
    }

    @Data
    public static class ServicePrincipalConfig {
        private String tenantId;
        private String clientId;
        private String clientSecret;
    }

    @Data
    public static class S3Config {
        private String accessKey;
        private String secretKey;
        private String region = "eu-west-1";
        /** Leave empty for AWS S3; set for MinIO or other S3-compatible stores. */
        private String endpoint;
        private boolean pathStyleAccess = false;
    }

    private AdlsConfig adls;
    private S3Config s3;
}
