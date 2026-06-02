package com.datahondo.flink.streaming.savepoint;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Orchestrates savepoint operations by delegating HTTP calls to {@link FlinkRestClient}
 * and applying the poll-until-done pattern.
 *
 * <p>Usage flow:
 * <ol>
 *   <li>Call {@link #triggerSavepoint} with the Flink host/port, job ID, and target directory.</li>
 *   <li>The service posts a savepoint request and polls the Flink REST API until the savepoint
 *       reaches a terminal state ({@code COMPLETED} or {@code FAILED}).</li>
 *   <li>Returns a {@link SavepointRecord} containing the savepoint path on success.</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SavepointService {

    private static final long POLL_INTERVAL_MS = 2_000L;

    private final FlinkRestClient restClient;

    /**
     * Triggers a savepoint for the given Flink job and blocks until it completes.
     *
     * @param host        Flink JobManager hostname
     * @param port        Flink REST API port
     * @param jobId       Flink job ID (UUID hex)
     * @param jobName     logical job name used by this application
     * @param targetDir   directory where Flink will write the savepoint
     * @param cancelJob   if {@code true} the job is atomically cancelled after the savepoint
     * @param timeoutMs   maximum time (ms) to wait for the savepoint to complete
     * @return a completed {@link SavepointRecord}
     * @throws SavepointException if the savepoint fails or the timeout is exceeded
     */
    public SavepointRecord triggerSavepoint(String host, int port,
                                            String jobId, String jobName,
                                            String targetDir, boolean cancelJob,
                                            long timeoutMs) throws SavepointException {
        log.info("[SAVEPOINT] Triggering savepoint for job '{}' (id={}) cancelJob={}",
                jobName, jobId, cancelJob);
        try {
            String requestId = restClient.postSavepointRequest(host, port, jobId, targetDir, cancelJob);
            String savepointPath = pollUntilCompleted(host, port, jobId, requestId, timeoutMs);

            SavepointRecord record = SavepointRecord.builder()
                    .jobName(jobName)
                    .jobId(jobId)
                    .savepointPath(savepointPath)
                    .createdAt(Instant.now())
                    .cancelledJob(cancelJob)
                    .build();

            log.info("[SAVEPOINT] Completed for job '{}' — path={}", jobName, savepointPath);
            return record;

        } catch (SavepointException e) {
            throw e;
        } catch (Exception e) {
            throw new SavepointException(
                    "Savepoint request failed for job '" + jobName + "': " + e.getMessage(), e);
        }
    }

    // ── private ───────────────────────────────────────────────────────────────

    private String pollUntilCompleted(String host, int port, String jobId,
                                      String requestId, long timeoutMs) throws SavepointException {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            FlinkRestClient.SavepointStatusResponse status;
            try {
                status = restClient.getSavepointStatus(host, port, jobId, requestId);
            } catch (Exception e) {
                throw new SavepointException("Failed to poll savepoint status: " + e.getMessage(), e);
            }

            switch (status.status) {
                case "COMPLETED":
                    return status.location;
                case "FAILED":
                    throw new SavepointException(
                            "Flink reported savepoint FAILED for requestId=" + requestId);
                default: // IN_PROGRESS or unknown — keep waiting
                    break;
            }

            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new SavepointException("Savepoint polling interrupted", ie);
            }
        }

        throw new SavepointException(
                "Savepoint timed out after " + timeoutMs + " ms (requestId=" + requestId + ")");
    }
}
