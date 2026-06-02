package com.datahondo.flink.streaming.savepoint;

/**
 * Abstraction over the Flink REST API calls required for savepoint operations.
 *
 * <p>Separating the HTTP concerns from the savepoint business logic makes
 * {@link SavepointService} independently unit-testable via mocks.
 */
public interface FlinkRestClient {

    /**
     * Sends a savepoint trigger request to the Flink JobManager.
     *
     * <p>Corresponds to:
     * {@code POST http://{host}:{port}/v1/jobs/{jobId}/savepoints}
     *
     * @param host       Flink JobManager hostname
     * @param port       Flink REST API port (usually 8081)
     * @param jobId      Flink job ID (UUID hex string)
     * @param targetDir  directory where Flink will write the savepoint
     * @param cancelJob  if {@code true} the job is atomically cancelled after the savepoint
     * @return the Flink-assigned {@code request-id} for polling
     * @throws Exception on HTTP or parse error
     */
    String postSavepointRequest(String host, int port, String jobId,
                                String targetDir, boolean cancelJob) throws Exception;

    /**
     * Polls the status of an in-progress savepoint request.
     *
     * <p>Corresponds to:
     * {@code GET http://{host}:{port}/v1/jobs/{jobId}/savepoints/{requestId}}
     *
     * @return a {@link SavepointStatusResponse} with status ({@code IN_PROGRESS}, {@code COMPLETED},
     *         or {@code FAILED}) and the savepoint location when completed
     * @throws Exception on HTTP or parse error
     */
    SavepointStatusResponse getSavepointStatus(String host, int port,
                                               String jobId, String requestId) throws Exception;

    // ── value object ──────────────────────────────────────────────────────────

    /**
     * Parsed response from the Flink savepoint status endpoint.
     */
    final class SavepointStatusResponse {
        /** One of {@code IN_PROGRESS}, {@code COMPLETED}, {@code FAILED}. */
        public final String status;
        /** Savepoint path — non-null only when {@code status} is {@code COMPLETED}. */
        public final String location;

        public SavepointStatusResponse(String status, String location) {
            this.status = status;
            this.location = location;
        }
    }
}
