package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.SchemaConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TTL-based cache wrapping {@link SchemaRegistryClient}.
 * Avoids repeated network calls for the same subject/version during a job run.
 * Cache is invalidated after {@link SchemaConfig#getCacheTtlMs()} or when a 404 is received.
 */
@Slf4j
public class CachedSchemaRegistryClient {

    private final SchemaRegistryClient delegate;
    private final long ttlMs;

    private final ConcurrentHashMap<String, CachedEntry> cache = new ConcurrentHashMap<>();

    public CachedSchemaRegistryClient(SchemaConfig config) {
        this.delegate = new SchemaRegistryClient(config);
        this.ttlMs = config.getCacheTtlMs();
    }

    /**
     * Returns the cached schema, fetching from the registry if the entry is absent or expired.
     *
     * @param subject schema subject name
     * @param version "latest" or numeric version
     */
    public String getSchema(String subject, String version) throws IOException {
        String key = subject + "@" + version;
        CachedEntry entry = cache.get(key);

        if (entry != null && !entry.isExpired(ttlMs)) {
            log.debug("[SCHEMA-REGISTRY-CACHE] Cache hit for {}", key);
            return entry.schema;
        }

        log.debug("[SCHEMA-REGISTRY-CACHE] Cache miss for {} — fetching from registry", key);
        try {
            String schema = delegate.fetchSchema(subject, version);
            cache.put(key, new CachedEntry(schema));
            return schema;
        } catch (IOException e) {
            // 404 / auth failure — remove stale entry if present
            if (e.getMessage() != null && (e.getMessage().contains("not found")
                    || e.getMessage().contains("authentication"))) {
                cache.remove(key);
            }
            throw e;
        }
    }

    /** Explicitly removes a subject from the cache (e.g., on schema evolution). */
    public void invalidate(String subject) {
        cache.keySet().removeIf(k -> k.startsWith(subject + "@"));
        log.info("[SCHEMA-REGISTRY-CACHE] Invalidated cache for subject={}", subject);
    }

    public void close() throws IOException {
        delegate.close();
    }

    private static final class CachedEntry {
        final String schema;
        final long fetchedAtMs;

        CachedEntry(String schema) {
            this.schema = schema;
            this.fetchedAtMs = System.currentTimeMillis();
        }

        boolean isExpired(long ttlMs) {
            return System.currentTimeMillis() - fetchedAtMs > ttlMs;
        }
    }
}
