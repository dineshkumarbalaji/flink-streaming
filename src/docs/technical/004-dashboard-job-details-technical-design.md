# Technical Design: Dashboard Job Details & Audit Integration

## Components

| Component | Class | Responsibility |
|---|---|---|
| In-Memory Cache | `InMemoryAuditCache` | Stores a bounded ring-buffer of recent `AuditEvent`s and `ReconciliationReport`s per job |
| Event Interceptor | `AuditService` | Forwards emitted events to `InMemoryAuditCache` before forwarding to the selected `AuditSink` |
| REST API | `JobController` | Exposes `/api/jobs/{name}` and related `/audit` and `/reconciliation` paths querying the cache and config storage |
| Dashboard UI | `app.js` | Invokes the new HTTP endpoints to populate modal UI overlay components without breaking active logic |

## Data Flow

```
[StreamingJobOrchestrator] → AuditService.emit(event)
  → AuditSink [Kafka/LOG]
  → InMemoryAuditCache.putEvent(jobName, event) (bounded update)

[User Clicks "test_job" Table Row in UI]
  → GET /api/jobs/test_job (File read configs/test_job.json)
  → GET /api/jobs/test_job/audit (Queries InMemoryAuditCache)
  → GET /api/jobs/test_job/reconciliation (Queries InMemoryAuditCache)
```

## Schema Handling

- **Type**: Dynamic JSON payload directly passed to UI.
- No structural schema changes imposed on core operators.

## Configuration

No new `StreamingJobConfig` configurations added. In-memory cache sizing acts dynamically governed by hard-coded or application properties if configured later, defaulting to max 50 events per job stringency constraints.

## Error Handling

| Error Type | Strategy |
|---|---|
| File not found for JSON Config | Catch `FileNotFoundException`; return generic 404; UI shows "Config Unavailable" |
| Empty Audit Logs | UI safely handles empty JSON arrays rendering "No Audit Data" |

## Performance Considerations

### Parallelism & Memory
- Caching objects requires robust concurrency controls since Flink pipelines fire Audit events asynchronously utilizing multiple JVM threads.
- `ConcurrentHashMap` combined with synchronized `ArrayDeque` restricts memory bloat while preventing ConcurrentModificationExceptions during iterative UI pulls.
