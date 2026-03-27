# Feature: Checkpoint Directory UI Configuration

## Business Objective
Operators need the ability to specify a custom Flink checkpoint storage path (e.g., an S3 bucket or shared HDFS path) when submitting a job via the UI. Without this, checkpoints always use the Flink cluster default, making recovery across restarts unreliable in production environments.

## Input
- **Source type**: UI form input (REST API `POST /api/jobs/submit`)
- **Field**: `checkpointDir` (optional string)
- **Examples**:
  - `s3://my-bucket/flink-checkpoints`
  - `file:///tmp/flink-checkpoints`
  - `hdfs://namenode:9000/checkpoints`
- **Default behaviour**: If left blank, falls back to the value of `FLINK_CHECKPOINT_DIR` env var configured in `application.yml`. If that is also empty, the Flink cluster default is used.

## Processing Logic
- The UI form exposes a "Checkpoint Directory (optional)" text input below the checkpoint interval field.
- On form submit, `checkpointDir` is included in the `JobRequest` JSON payload.
- `JobController.mapToConfig()` applies the following priority:
  1. Per-job value from the request (if non-empty)
  2. System value from `application.yml` → `streaming.job.flink.checkpoint-dir`
- The resolved value is set on `FlinkConfig.checkpointDir`.
- `StreamingJobOrchestrator` reads `FlinkConfig.checkpointDir` and configures the Flink `StreamExecutionEnvironment` accordingly.

## Output
- **Target system**: Flink checkpoint storage
- **Effect**: Flink writes checkpoint snapshots to the specified path, enabling job recovery after restart or failure.

## Edge Cases

| Scenario | Handling Strategy |
|---|---|
| `checkpointDir` is blank in UI | Falls back to `application.yml` value; no error |
| `application.yml` value is also empty | Flink uses cluster-level default checkpoint storage |
| Path is not accessible by TaskManagers | Job fails at first checkpoint; Flink logs the storage error |
| Path uses unsupported scheme | Flink throws at startup; job submission fails with clear error |

## Test Coverage
- Unit test file: `src/test/java/.../web/JobControllerTest.java`
- Key scenarios:
  - `mapToConfig_usesRequestCheckpointDir_whenProvided`
  - `mapToConfig_fallsBackToSystemCheckpointDir_whenRequestValueIsNull`
