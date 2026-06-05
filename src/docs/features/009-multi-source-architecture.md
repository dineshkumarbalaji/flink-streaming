# Feature 009 — Multi-Source Architecture

**Feature ID:** 009
**Status:** Done
**Type:** Feature
**Date:** 2026-06-05

---

## Overview

Extends the source layer from a single Kafka implementation to a pluggable multi-source
architecture supporting File (batch / streaming), JDBC, and REST API polling — each wired
through the same `SourceLayer` interface so the orchestrator and transformation layer are
unchanged.

---

## Source Type Discriminator

`SourceConfig.type` selects the implementation at job-submit time:

| Value | Layer class | Swim lane |
|-------|-------------|-----------|
| `KAFKA` (default) | `KafkaSourceLayer` | Soft — event-driven |
| `FILE` | `FileSourceLayer` | Hard — batch / streaming files |
| `JDBC` | `JdbcSourceLayer` | Hard — structured DB read |
| `API` | `ApiSourceLayer` | Soft — REST polling |

---

## 009-A: File Source (`FileSourceLayer`)

### What it does
Reads CSV, JSON, or Parquet files from local filesystem, Azure ADLS Gen2, or AWS S3.
Registers a Flink Table API `filesystem` connector view — no DataStream API imports needed.

### Storage backends

| URI prefix | Backend | Credentials config |
|------------|---------|-------------------|
| Bare path or `file:///` | Local filesystem | None |
| `abfs://` / `abfss://` | Azure ADLS Gen2 | `storage.adls.accountKey` or `storage.adls.servicePrincipal` |
| `s3://` / `s3a://` | AWS S3 | `storage.s3.accessKey` + `storage.s3.secretKey` |

S3 paths are normalised to `s3a://` automatically via `StoragePathResolver.normalise()`.

### Configuration

```yaml
streaming.job.sources[0]:
  type: FILE
  tableName: orders
  fileFormat: CSV               # CSV | JSON | PARQUET
  storagePath: abfs://raw@myaccount.dfs.core.windows.net/orders/
  recursive: false
  monitorInterval: 0            # 0 = one-shot batch; >0 ms = continuous watching
  storage:
    adls:
      accountName: myaccount
      accountKey: ${ADLS_KEY}
    s3:
      accessKey: ${AWS_ACCESS_KEY}
      secretKey: ${AWS_SECRET_KEY}
      region: eu-west-1
  schema:
    fields:
      - { name: id,   type: INT }
      - { name: name, type: STRING }
```

### Key classes

| Class | Role |
|-------|------|
| `FileSourceLayer` | Implements `SourceLayer`; builds filesystem DDL; registers view |
| `StoragePathResolver` | Detects URI scheme; injects Hadoop FS credentials into Flink env |

---

## 009-B: JDBC Source (`JdbcSourceLayer`)

### What it does
Reads from any JDBC datasource (PostgreSQL, MySQL, Oracle, H2) via a SELECT query.
Uses Flink's Table API JDBC connector DDL — driver auto-detected from the URL prefix.

### Configuration

```yaml
streaming.job.sources[0]:
  type: JDBC
  tableName: customers
  jdbcUrl: jdbc:postgresql://host:5432/mydb
  query: "SELECT id, name, email FROM customers WHERE active = true"
  fetchSize: 1000
  jdbcUsername: ${DB_USERNAME}
  jdbcPassword: ${DB_PASSWORD}
  sslMode: require              # disable | require | verify-full
  schema:
    fields:
      - { name: id,    type: INT }
      - { name: name,  type: STRING }
      - { name: email, type: STRING }
```

### Driver auto-detection

| URL prefix | Driver class |
|------------|-------------|
| `jdbc:postgresql:` | `org.postgresql.Driver` |
| `jdbc:mysql:` | `com.mysql.cj.jdbc.Driver` |
| `jdbc:oracle:` | `oracle.jdbc.driver.OracleDriver` |
| `jdbc:h2:` | `org.h2.Driver` |

---

## 009-C: API Source (`ApiSourceLayer`)

### What it does
Polls a REST endpoint on a configurable interval with **at-least-once delivery** via Flink
checkpoint state. Supports four authentication mechanisms.

### Authentication

| Type | How it works |
|------|-------------|
| `BEARER` | Static `Authorization: Bearer <token>` header |
| `OAUTH2` | Client credentials flow; token cached and refreshed 60 s before expiry |
| `MTLS` | Client certificate presented on TLS handshake via PKCS#12 keystore |
| `API_KEY` | Key in request header (default `X-Api-Key`) or query parameter |

### Configuration

```yaml
streaming.job.sources[0]:
  type: API
  tableName: prices
  url: https://api.example.com/prices
  method: GET
  pollIntervalMs: 5000
  jsonPath: $.data[*]           # JSONPath to extract records array; omit for root
  retryAttempts: 3
  retryBackoffMs: 500           # exponential: 500, 1000, 2000 ms
  apiAuth:
    type: OAUTH2                # BEARER | OAUTH2 | MTLS | API_KEY
    tokenUrl: https://auth.example.com/oauth/token
    clientId: ${OAUTH_CLIENT_ID}
    clientSecret: ${OAUTH_CLIENT_SECRET}
    scope: read:prices
```

### Checkpoint state
`RestPollingSourceFunction` stores the last-polled epoch-ms in `ListState<Long>`.
On restart, the source replays from the last checkpointed cursor — providing at-least-once delivery.

### Key classes

| Class | Role |
|-------|------|
| `ApiSourceLayer` | Implements `SourceLayer`; wires `RestPollingSourceFunction`; validates config |
| `RestPollingSourceFunction` | `RichSourceFunction` + `CheckpointedFunction`; polling loop; auth |
| `ApiAuthConfig` | Config POJO: `BEARER / OAUTH2 / MTLS / API_KEY` discriminator + all fields |
| `OAuthTokenManager` | Fetches + caches OAuth2 client credentials token; proactive refresh |
| `HttpClientFactory` | Builds `CloseableHttpClient`; wires mTLS keystore + truststore |

---

## Orchestrator Dispatch

`StreamingJobOrchestrator` no longer accepts a single `SourceLayer` bean.
Spring injects all `SourceLayer` implementations as a `List<SourceLayer>`:

```java
@Autowired
public StreamingJobOrchestrator(List<SourceLayer> sourceLayerList, ...) {
    this.sourceLayers = sourceLayerList.stream()
        .collect(Collectors.toMap(s -> s.getSourceType().toUpperCase(), s -> s));
}
```

`SourceLayer.getSourceType()` returns `"KAFKA"` by default (backward compatible).
New layers override it: `FileSourceLayer` → `"FILE"`, `JdbcSourceLayer` → `"JDBC"`,
`ApiSourceLayer` → `"API"`.

---

## Validation (JobController)

`/validate` and `/submit` endpoints validate per source type before dispatching:

| Type | Validated fields |
|------|-----------------|
| `FILE` | `storagePath` non-blank; `fileFormat` in {CSV, JSON, PARQUET} |
| `JDBC` | `jdbcUrl` non-blank; `query` non-blank; `tableName` non-blank |
| `API` | `url` valid URI; `apiAuth.type` set; OAUTH2 requires `tokenUrl` + `clientId` |

---

## New `pom.xml` Dependencies

| Artifact | Version | Used by |
|----------|---------|---------|
| `flink-connector-files` | `1.18.0` | FileSourceLayer Table API DDL |
| `flink-connector-jdbc` | `3.1.2-1.17` | JdbcSourceLayer + JdbcTargetLayer |
| `mysql-connector-java` | `8.0.33` | MySQL JDBC driver (runtime) |
| `httpclient` | `4.5.14` | ApiSourceLayer (Java 8 compatible) |
| `json-path` | `2.9.0` | JSONPath extraction in RestPollingSourceFunction |

---

## Test Coverage

| Test class | Methods | Coverage |
|------------|---------|---------|
| `StoragePathResolverTest` | 11 | URI scheme detection for LOCAL/ADLS/S3; normalise() |
| `FileSourceLayerTest` | 6 | Validation: null/blank path, null table, invalid format |
| `JdbcSourceLayerTest` | 4 | Validation: null URL, null query, null table |
| `ApiSourceLayerTest` | 5 | Validation: null/blank URL, null table, OAUTH2 missing tokenUrl, invalid URI |
| `OAuthTokenManagerTest` | 2 | Constructor; unreachable token endpoint |

---

## Known Limitations

- ADLS Gen2 and S3 filesystem plugins must be installed in Flink's `plugins/` directory
  on all cluster nodes. They are not Maven dependencies — they are loaded at runtime.
- `ApiSourceLayer` uses polling (pull); true push/webhook delivery requires a Flink REST
  source or Kafka bridge. Planned for a future iteration.
- `JdbcSourceLayer` reads a full snapshot at job start; CDC (change data capture) is not
  supported in this release.
