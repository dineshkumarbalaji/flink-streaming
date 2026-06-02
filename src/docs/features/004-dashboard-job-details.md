# Dashboard Job Details & Audit Tracking

## Business Objective
Developers and operators need real-time line-of-sight into the configurations and lifecycle of their running streaming applications directly from the Flink Control web dashboard. This feature adds a job details pane to visualize the exact configuration that was deployed, the history of audit events (e.g. JOB_SUBMITTED, JOB_FAILED), and the latest pipeline reconciliation checks (source read vs target written), reducing debugging time and eliminating the need to search through external logging systems.

## Input
- **Source type**: UI Clicks (API Requests)
- **Topic / Path**: `/api/jobs/{jobName}/*` REST endpoints
- **Format**: JSON requests from Frontend UI

## Processing Logic
- The framework intercepts emitted `AuditEvent` and `ReconciliationReport` models via normal execution and caches them within a bounded concurrent queue in memory.
- When the UI requests job details, it makes three calls:
  1. Retrieves the static configuration payload from standard storage (`configs/{jobName}.json`)
  2. Queries the `InMemoryAuditCache` for recent `AuditEvent` arrays.
  3. Queries the `InMemoryAuditCache` for the latest `ReconciliationReport`.
- Caching is LRU (evict oldest) capped per-job name preventing memory leaks.
- The UI binds the details into a contextual modal window over the active jobs table.

## Output
- **Target system**: Flink Control Dashboard UI
- **Output schema**:
  ```json
  // Structure for Audit response
  [
    {
      "runId": "dev-1234",
      "jobName": "test_job",
      "eventType": "JOB_RUNNING",
      "stage": "orchestrator",
      "count": 0,
      "metadata": { "flinkJobId" : "..." },
      "timestamp": "2024-10-10:T10:00:00Z"
    }
  ]
  ```

## Edge Cases

| Scenario | Handling Strategy |
|---|---|
| Cache memory bounds exceeded | Uses a bounded `Deque` dropping oldest audit events |
| Job restarted / New RunId | The cache will capture events for the latest run natively; UI groups by timeline |
| Config file deleted | API handles gracefully / returns 404 cleanly, UI displays empty config |
| Restart Service | In-memory cache is ephemeral, prior data is lost; relied on external sinks (Log/Kafka) for durable storage |

## Test Coverage
- Unit test file: `src/test/java/com/datahondo/flink/streaming/audit/InMemoryAuditCacheTest.java`
- Unit test file: `src/test/java/com/datahondo/flink/streaming/web/JobControllerTest.java`
- Key scenarios covered: Cache eviction logic, thread safety, endpoint payload mapping correctly to valid models.
