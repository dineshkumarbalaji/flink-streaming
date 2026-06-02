# Feature 008 — Job Audit Table & Dashboard

**Feature ID:** 008
**Status:** Done
**Type:** Feature
**Date:** 2026-06-02

---

## Overview

Adds a persistent job audit table and REST API so operators have full visibility
into every job submission — including historical runs, live status, config
snapshots, and stop/delete controls — without inspecting JobManager logs directly.

Also upgrades the web dashboard with a **Job History** panel that auto-refreshes
every 30 seconds.

---

## Components Added

| Class | Package | Role |
|-------|---------|------|
| `JobAuditRecord` | `job.audit` | JPA entity — one row per job submission |
| `JobAuditRepository` | `job.audit` | Spring Data JPA — findByJobName, findByStatus, findAll |
| `JobAuditService` | `job.audit` | CRUD: createRecord, updateRunning, updateStatus, deleteById |
| `JobStatusPoller` | `job.audit` | `@Scheduled` every 30 s — polls live JobClient callbacks |
| `JobDashboardController` | `web` | REST endpoints for dashboard queries and job control |

---

## Database Schema (auto-created via JPA ddl-auto: update)

```sql
CREATE TABLE job_audit_records (
    id                  BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_name            VARCHAR(256) NOT NULL,
    flink_job_id        VARCHAR(128),
    run_id              VARCHAR(128),
    status              VARCHAR(32) NOT NULL DEFAULT 'SUBMITTING',
    parallelism         INTEGER,
    checkpoint_interval BIGINT,
    config_file_path    VARCHAR(512),
    config_snapshot     TEXT,
    submitted_at        TIMESTAMP,
    updated_at          TIMESTAMP,
    error_message       TEXT
);
```

Default datasource: H2 file (`./data/audit-db`). Switch to PostgreSQL via
`DB_URL`, `DB_DRIVER`, `DB_USERNAME`, `DB_PASSWORD` environment variables.

---

## REST Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/dashboard/jobs` | All records, newest first |
| GET | `/api/dashboard/jobs/{id}` | Single audit record |
| GET | `/api/dashboard/jobs/by-name/{name}` | History for a job name |
| POST | `/api/dashboard/jobs/{id}/stop` | Cancel running job (409 if not RUNNING) |
| DELETE | `/api/dashboard/jobs/{id}` | Delete finished/failed/cancelled record |

---

## Status Lifecycle

```
SUBMITTING → RUNNING → FINISHED
                ├──→ FAILED
                └──→ CANCELLED
```

Transitions are driven by:
- `JobController.submitJob()` — creates SUBMITTING, updates to RUNNING after `executeAsync()`
- `JobStatusPoller` — polls every 30 s via registered `JobClient` callbacks
- `JobDashboardController.stopJob()` — sets CANCELLED on user request

---

## Configuration

```yaml
streaming:
  audit:
    poll-interval-ms: 30000   # status poll frequency (ms)
    poll-enabled: true

spring:
  datasource:
    url: ${DB_URL:jdbc:h2:file:./data/audit-db;DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE}
  jpa:
    hibernate:
      ddl-auto: update
```

---

## UI Changes

- **Job History** panel added to the Dashboard tab (`index.html`)
- Shows ID, Job Name, Flink Job ID (truncated), Status (color-coded), Parallelism, Submitted
- Stop button (RUNNING jobs only), Delete button (terminal jobs)
- Auto-refreshes every 30 s when Dashboard tab is active

---

## Known Limitations

- Restart (resubmit from saved config snapshot) deferred to Feature 009.
- `JobStatusPoller` callbacks are in-memory; lost on app restart. Flink REST
  fallback polling (using stored `flinkJobId`) planned for Feature 009.
- Config snapshots include full request JSON; passwords are redacted server-side
  before the config file is written to disk but are present in the DB snapshot
  — use DB-level encryption or a secrets manager for production.
