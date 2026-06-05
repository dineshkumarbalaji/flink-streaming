# Feature 012 — Prometheus & Grafana Monitoring

**Feature ID:** 012
**Status:** Done
**Type:** Feature
**Date:** 2026-06-05

---

## Overview

Wires Flink's built-in Prometheus metrics reporter into `docker-compose.yml` and provisions
a pre-built Grafana dashboard covering the four cross-cutting concerns from the architecture
diagram: **Audit** (record counts), **Reconciliation** (latency/mismatch), **Checkpoint**
(duration), and **Metrics** (throughput, errors, DLQ).

---

## Service URLs

| Service | URL | Default credentials |
|---------|-----|-------------------|
| Prometheus | `http://localhost:9090` | None |
| Grafana | `http://localhost:3000` | admin / `${GRAFANA_PASSWORD:-admin}` |

---

## Prometheus Configuration

### Retention and scrape

```yaml
# docker-compose.yml — prometheus service
command:
  - '--storage.tsdb.retention.time=30d'   # 30-day metric history
  - '--web.enable-lifecycle'               # hot-reload config via POST /-/reload
```

`monitoring/prometheus.yml`:
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: flink-jobmanager
    static_configs:
      - targets: ['jobmanager:9249']
  - job_name: flink-taskmanager
    static_configs:
      - targets: ['taskmanager:9249']
  - job_name: flink-app
    static_configs:
      - targets: ['flink-app:8082']
```

### Flink reporter config

Injected via `FLINK_PROPERTIES` in docker-compose for both jobmanager and taskmanager:

```
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249
metrics.latency.interval: 5000
```

---

## Grafana Dashboard

Auto-provisioned at startup via `monitoring/grafana/provisioning/`. No manual import needed.

### Dashboard panels

| Panel | Metric | Visualisation |
|-------|--------|--------------|
| Records Read / sec | `flink_taskmanager_job_task_operator_numRecordsInPerSecond` | Time series |
| Records Written / sec | `flink_taskmanager_job_task_operator_numRecordsOutPerSecond` | Time series |
| Schema Rejections / sec | `rate(flink_pipeline_source_records_rejected_total[1m])` | Time series |
| DLQ Records Routed / sec | `rate(flink_pipeline_dlq_records_routed_total[1m])` | Time series |
| Sink Failures / sec | `rate(flink_pipeline_sink_records_failed_total[1m])` | Time series + alert |
| Last Checkpoint Duration | `flink_jobmanager_job_lastCheckpointDuration` | Gauge |
| Running Jobs | `flink_jobmanager_numRunningJobs` | Stat |
| End-to-End Latency (p50/p99) | Flink latency histogram | Time series |

### Custom pipeline metrics (for future instrumentation)

These metric names are reserved for application-level counters to be wired into source/sink
layers in a future iteration:

| Metric name | Type | Labels |
|-------------|------|--------|
| `flink_pipeline_source_records_read_total` | Counter | `job_name`, `source_table` |
| `flink_pipeline_source_records_rejected_total` | Counter | `job_name`, `source_table` |
| `flink_pipeline_transform_records_out_total` | Counter | `job_name` |
| `flink_pipeline_sink_records_written_total` | Counter | `job_name`, `sink_type`, `sink_target` |
| `flink_pipeline_sink_records_failed_total` | Counter | `job_name`, `sink_type` |
| `flink_pipeline_dlq_records_routed_total` | Counter | `job_name`, `source_table`, `error_type` |

---

## File Structure

```
monitoring/
├── prometheus.yml                               # Prometheus scrape config
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── prometheus.yml                   # Auto-wires Prometheus datasource
    │   └── dashboards/
    │       └── dashboards.yml                   # Dashboard file provider config
    └── dashboards/
        └── flink-pipeline.json                  # Pre-built 8-panel dashboard
```

---

## docker-compose Changes

```yaml
# New services added
prometheus:
  image: prom/prometheus:v2.51.0
  ports: ["9090:9090"]
  volumes:
    - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    - prometheus_data:/prometheus       # persisted volume for 30-day retention

grafana:
  image: grafana/grafana:10.3.0
  ports: ["3000:3000"]
  volumes:
    - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
    - ./monitoring/grafana/dashboards:/etc/grafana/dashboards:ro
    - grafana_data:/var/lib/grafana

# New named volumes
volumes:
  prometheus_data:
  grafana_data:
```

---

## New `pom.xml` Dependency

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-metrics-prometheus</artifactId>
    <version>1.18.0</version>
</dependency>
```

This provides `PrometheusReporterFactory` which Flink loads via the metrics reporter
SPI at cluster startup. No code changes are needed in the application — the reporter
is configured entirely through `FLINK_PROPERTIES`.

---

## Known Limitations

- Custom pipeline metric counters (`flink_pipeline_source_records_*`) are reserved but
  not yet wired into the source/sink layers. Flink's built-in operator metrics
  (`numRecordsIn/OutPerSecond`) are available immediately.
- Grafana runs without authentication by default in docker-compose (`admin/admin`).
  Set `GRAFANA_PASSWORD` env var or configure OAuth2 before exposing externally.
- Prometheus data is stored in a Docker named volume — it is not backed up automatically.
  Configure external object storage (e.g. Thanos, Cortex) for production-grade retention.
