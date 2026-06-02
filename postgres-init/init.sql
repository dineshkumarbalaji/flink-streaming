-- Database Initialization for Flink Audit and Reconciliation

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
