-- Database Initialization for Flink Audit, Reconciliation, and Job Audit (Feature 008)

-- Feature 008: persistent job submission history
CREATE TABLE IF NOT EXISTS job_audit_records (
    id                  BIGSERIAL PRIMARY KEY,
    job_name            VARCHAR(256) NOT NULL,
    flink_job_id        VARCHAR(128),
    run_id              VARCHAR(128),
    status              VARCHAR(32)  NOT NULL DEFAULT 'SUBMITTING',
    parallelism         INTEGER,
    checkpoint_interval BIGINT,
    config_file_path    VARCHAR(512),
    config_snapshot     TEXT,
    submitted_at        TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    error_message       TEXT
);

CREATE TABLE IF NOT EXISTS flink_audit_events (
    run_id        VARCHAR(128),
    job_name      VARCHAR(256),
    event_type    VARCHAR(64),
    stage         VARCHAR(256),
    count         BIGINT,
    ts            TIMESTAMP,
    metadata      TEXT
);

CREATE TABLE IF NOT EXISTS flink_reconciliation_reports (
    run_id               VARCHAR(128),
    job_name             VARCHAR(256),
    window_start         TIMESTAMP,
    window_end           TIMESTAMP,
    window_label         VARCHAR(32),
    source_read_count    BIGINT,
    schema_rejected_count BIGINT,
    transformed_count    BIGINT,
    target_written_count BIGINT,
    reconciled           BOOLEAN,
    discrepancies        TEXT
);
