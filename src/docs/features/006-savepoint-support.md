# Feature 006 — Savepoint Support

**Status:** Done  
**Type:** Feature

---

## Overview

Adds first-class savepoint support to the Flink streaming control plane.  
Savepoints are separate from checkpoints — they are operator-state snapshots triggered
explicitly by the user for controlled job upgrades, migrations, or rollbacks.

---

## User Stories

| # | Story |
|---|---|
| 1 | As an operator, I can take a savepoint of a running job from the Dashboard without stopping it |
| 2 | As an operator, I can see all savepoints for a job (path, timestamp) in the Dashboard |
| 3 | As an operator, I can copy a savepoint path with one click to use it when resubmitting |
| 4 | As an operator, I can cancel a job using the dedicated Cancel button (separate from savepoint) |
| 5 | As a developer, I can resubmit a job with a savepoint path to restore prior state |

---

## Architecture

```
Dashboard
  ├── "Take Savepoint" button  ──▶  POST /api/jobs/{jobName}/savepoint
  ├── "Cancel Job" button      ──▶  DELETE /api/jobs/{jobName}
  └── Savepoints panel         ──▶  GET  /api/jobs/{jobName}/savepoints

Submit form
  └── Restore from Savepoint field  ──▶  POST /api/jobs/submit  (savepointPath in body)

JobController
  └── triggerSavepoint()  ──▶  StreamingJobOrchestrator.triggerSavepoint()
                          ──▶  SavepointService.triggerSavepoint()
                          ──▶  FlinkRestClient.postSavepointRequest()    ─▶  Flink REST API
                                             .getSavepointStatus()  (polling)

StreamingJobOrchestrator.submitJob()
  └── buildFlinkConfiguration()  ─▶  sets execution.savepoint.path in Flink Configuration
                                 ─▶  sets execution.savepoint.ignore-unclaimed-state
```

---

## New Components

| Class | Package | Role |
|---|---|---|
| `SavepointRecord` | `savepoint` | Immutable model: jobName, jobId, path, timestamp, cancelledJob flag |
| `SavepointException` | `savepoint` | Checked exception for savepoint failures/timeouts |
| `FlinkRestClient` | `savepoint` | Interface for Flink REST API HTTP calls |
| `HttpFlinkRestClient` | `savepoint` | Java 8 `HttpURLConnection` implementation of `FlinkRestClient` |
| `SavepointService` | `savepoint` | Business logic: trigger + poll-until-done |
| `SavepointRegistry` | `savepoint` | Thread-safe in-memory store of `SavepointRecord` per job |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/jobs/{jobName}/savepoint` | Trigger a savepoint (non-cancelling). Blocks until done or timeout. Returns `SavepointRecord`. |
| `GET` | `/api/jobs/{jobName}/savepoints` | List all savepoints for a job, oldest first. |
| `POST` | `/api/jobs/submit` | Existing endpoint; now accepts optional `savepointPath` + `allowNonRestoredState` in body. |

---

## Configuration

| YAML key | Env var | Default | Description |
|---|---|---|---|
| `streaming.job.flink.savepoint-dir` | `FLINK_SAVEPOINT_DIR` | `/app/checkpoints/savepoints` | Target directory for savepoints |
| `streaming.job.flink.savepoint-poll-timeout-ms` | `FLINK_SAVEPOINT_POLL_TIMEOUT_MS` | `300000` (5 min) | Max time to wait for savepoint completion |

The savepoint directory is stored on the shared `checkpoints` Docker volume (already mounted
across jobmanager, taskmanager, and flink-app from the docker-compose changes in Feature 005).

---

## Acceptance Criteria

- [ ] "Take Savepoint" and "Cancel Job" are separate buttons in the Dashboard job details modal
- [ ] Savepoint panel shows path, timestamp, and copy-to-clipboard button for each savepoint
- [ ] `POST /api/jobs/{jobName}/savepoint` returns `SavepointRecord` with the saved path
- [ ] `GET /api/jobs/{jobName}/savepoints` returns the full history for a job
- [ ] Submitting with `savepointPath` restores Flink job state from that savepoint
- [ ] `allowNonRestoredState=true` is only sent when the checkbox is checked
- [ ] Savepoint timeout is configurable via `FLINK_SAVEPOINT_POLL_TIMEOUT_MS`

---

## Tests Added

| Class | Method | Covers |
|---|---|---|
| `SavepointRegistryTest` | `register_andGetForJob_returnsRegisteredRecord` | Registry storage |
| `SavepointRegistryTest` | `register_multipleRecords_returnsAllInInsertionOrder` | Ordering |
| `SavepointRegistryTest` | `getLatest_returnsLastRegistered` | Latest lookup |
| `SavepointServiceTest` | `triggerSavepoint_returnsSavepointRecord_onSuccess` | Happy path |
| `SavepointServiceTest` | `triggerSavepoint_throwsException_whenSavepointFails` | Failure handling |
| `SavepointServiceTest` | `triggerSavepoint_throwsException_whenTimeoutExceeded` | Timeout |
| `SavepointServiceTest` | `triggerSavepoint_pollsUntilCompleted_afterInProgressResponse` | Poll loop |

---

## Known Limitations

- `SavepointRegistry` is in-memory only — savepoint history is lost on app restart.
  A future enhancement (007) should persist it alongside `configs/{jobName}.json`.
- `triggerSavepoint` blocks the HTTP thread for the duration of the poll loop.
  A future enhancement could make it async with a status-polling endpoint.
