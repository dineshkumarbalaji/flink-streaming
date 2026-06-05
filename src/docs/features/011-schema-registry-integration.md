# Feature 011 — Schema Registry Integration

**Feature ID:** 011
**Status:** Done
**Type:** Feature
**Date:** 2026-06-05

---

## Overview

Adds support for fetching Avro schemas from a **SASL-secured Confluent Schema Registry**
instead of supplying them inline in `SchemaConfig.definition`. A TTL-based cache prevents
repeated network calls during a job run. Inline schema paths are unchanged — no regression.

---

## When to use

Set `schemaDefinition.type: REGISTRY` when:
- Avro schemas are centrally managed in Confluent Schema Registry
- Schemas evolve independently of job config (schema evolution)
- Multiple jobs share the same schema subject

Leave as `INLINE` (or `JSON`) for self-contained JSON Schema or one-off Avro definitions.

---

## Configuration

```yaml
streaming.job.sources[0]:
  type: KAFKA
  topic: orders
  schemaDefinition:
    type: REGISTRY                  # INLINE (existing) | REGISTRY (new)
    registryUrl: https://schema-registry:8081
    subject: orders-value           # defaults to <topic>-value if omitted
    version: latest                 # numeric version or "latest"
    cacheTtlMs: 300000              # 5-minute cache TTL
    saslMechanism: SCRAM-SHA-256    # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
    registryUsername: ${SR_USERNAME}
    registryPassword: ${SR_PASSWORD}
    tls:
      enabled: true
      truststorePath: /app/certs/truststore.jks
      truststorePassword: ${SR_TRUSTSTORE_PASSWORD}
      skipHostnameVerification: false
```

---

## Authentication

All mechanisms use HTTP Basic (`Authorization: Basic base64(username:password)`) on every
request. The `saslMechanism` field documents the server-side protocol for operators and
future SCRAM-token enhancements — the HTTP transport layer always uses Basic auth over TLS.

| Mechanism | Server config | Notes |
|-----------|--------------|-------|
| `PLAIN` | `SASL_SSL` with `PLAIN` | Username + password in Basic header |
| `SCRAM-SHA-256` | `SASL_SSL` with `SCRAM-SHA-256` | Same HTTP Basic — SCRAM handshake is at the broker level, not the REST API |
| `SCRAM-SHA-512` | `SASL_SSL` with `SCRAM-SHA-512` | Same as above |

---

## TLS

When `tls.enabled: true`, the HTTP client loads the specified JKS truststore to verify the
registry server's certificate. Set `skipHostnameVerification: false` (default) to enforce
hostname matching. The TLS context is scoped to the registry client — it does not alter the
JVM global `SSLContext`.

---

## Caching

`CachedSchemaRegistryClient` wraps `SchemaRegistryClient` with a `ConcurrentHashMap` cache:

- **Cache hit:** Returns the stored schema string without a network call.
- **Cache miss / TTL expiry:** Fetches from the registry and updates the cache.
- **404 response:** Removes the stale entry to prevent serving a deleted subject.
- **Manual invalidation:** `POST /api/schema/refresh/{subject}` calls
  `CachedSchemaRegistryClient.invalidate(subject)` — planned endpoint for schema evolution.

---

## Key Classes

| Class | Package | Role |
|-------|---------|------|
| `SchemaRegistryClient` | `source` | HTTP client fetching schema by subject/version from the Registry REST API |
| `CachedSchemaRegistryClient` | `source` | TTL cache wrapping `SchemaRegistryClient`; 404 invalidation |
| `SchemaConfig.RegistryTlsConfig` | `config` | Inner class: `truststorePath`, `truststorePassword`, `skipHostnameVerification` |
| `SchemaConfig.SchemaField` | `config` | Inner class: `name`, `type`, `nullable`, `primaryKey` |

### `SchemaConfig` additions

```java
private List<SchemaField> fields;       // typed column list for JDBC/File source/sink
private long cacheTtlMs = 300_000L;
private String saslMechanism = "PLAIN";
private String registryUsername;
private String registryPassword;
private RegistryTlsConfig tls;
```

---

## Integration Points

- **`KafkaSourceLayer`**: when `SchemaConfig.type == REGISTRY`, creates a
  `CachedSchemaRegistryClient` and fetches the schema before building the Avro deserializer.
  Falls back to the existing inline path when `type == INLINE` or `AVRO`.
- **`SqlValidatorService`**: when `type == REGISTRY`, calls the cached client to build the
  `SourceEntry` used for SQL pre-validation — ensures multi-source JOINs are validated
  against the live registry schema.

---

## Test Coverage

| Test class | Methods | Coverage |
|------------|---------|---------|
| `CachedSchemaRegistryClientTest` | 3 | Constructor; unreachable registry; invalidate no-op |

---

## Known Limitations

- SCRAM-SHA-256/512 at the HTTP transport level requires a reverse proxy (e.g. Confluent REST
  proxy or Confluent Platform) that maps HTTP Basic credentials to SCRAM tokens. Direct SCRAM
  over raw TCP is not implemented — use `PLAIN` for standard Confluent Cloud / CP deployments.
- Schema evolution (new fields added to the subject) requires either a manual
  `invalidate()` call or waiting for the TTL to expire before the new schema is picked up.
- Authentication failures (HTTP 401/403) are surfaced as `IOException` — the job will fail
  at startup rather than silently serving stale or wrong schemas.
