package com.datahondo.flink.streaming.savepoint;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SavepointServiceTest {

    @Mock
    private FlinkRestClient restClient;

    private SavepointService service;

    private static final String HOST = "jobmanager";
    private static final int PORT = 8081;
    private static final String JOB_ID = "abc123def456";
    private static final String JOB_NAME = "test-job";
    private static final String TARGET_DIR = "file:///app/checkpoints/savepoints";
    private static final long TIMEOUT_MS = 10_000L;

    @BeforeEach
    void setUp() {
        service = new SavepointService(restClient);
    }

    // ── triggerSavepoint ──────────────────────────────────────────────────────

    @Test
    void triggerSavepoint_returnsSavepointRecord_onSuccess() throws Exception {
        when(restClient.postSavepointRequest(HOST, PORT, JOB_ID, TARGET_DIR, false))
                .thenReturn("req-001");
        when(restClient.getSavepointStatus(HOST, PORT, JOB_ID, "req-001"))
                .thenReturn(new FlinkRestClient.SavepointStatusResponse(
                        "COMPLETED", "file:///app/checkpoints/savepoints/savepoint-abc"));

        SavepointRecord record = service.triggerSavepoint(
                HOST, PORT, JOB_ID, JOB_NAME, TARGET_DIR, false, TIMEOUT_MS);

        assertNotNull(record);
        assertEquals(JOB_NAME, record.getJobName());
        assertEquals(JOB_ID, record.getJobId());
        assertEquals("file:///app/checkpoints/savepoints/savepoint-abc", record.getSavepointPath());
        assertFalse(record.isCancelledJob());
        assertNotNull(record.getCreatedAt());
    }

    @Test
    void triggerSavepoint_withCancelJob_flagsRecordCorrectly() throws Exception {
        when(restClient.postSavepointRequest(HOST, PORT, JOB_ID, TARGET_DIR, true))
                .thenReturn("req-002");
        when(restClient.getSavepointStatus(HOST, PORT, JOB_ID, "req-002"))
                .thenReturn(new FlinkRestClient.SavepointStatusResponse(
                        "COMPLETED", "file:///app/checkpoints/savepoints/savepoint-xyz"));

        SavepointRecord record = service.triggerSavepoint(
                HOST, PORT, JOB_ID, JOB_NAME, TARGET_DIR, true, TIMEOUT_MS);

        assertTrue(record.isCancelledJob());
    }

    @Test
    void triggerSavepoint_throwsException_whenSavepointFails() throws Exception {
        when(restClient.postSavepointRequest(HOST, PORT, JOB_ID, TARGET_DIR, false))
                .thenReturn("req-003");
        when(restClient.getSavepointStatus(HOST, PORT, JOB_ID, "req-003"))
                .thenReturn(new FlinkRestClient.SavepointStatusResponse("FAILED", null));

        assertThrows(SavepointException.class, () ->
                service.triggerSavepoint(HOST, PORT, JOB_ID, JOB_NAME, TARGET_DIR, false, TIMEOUT_MS));
    }

    @Test
    void triggerSavepoint_throwsException_whenTimeoutExceeded() throws Exception {
        when(restClient.postSavepointRequest(HOST, PORT, JOB_ID, TARGET_DIR, false))
                .thenReturn("req-004");
        when(restClient.getSavepointStatus(HOST, PORT, JOB_ID, "req-004"))
                .thenReturn(new FlinkRestClient.SavepointStatusResponse("IN_PROGRESS", null));

        // Timeout of 1 ms guarantees immediate expiry
        assertThrows(SavepointException.class, () ->
                service.triggerSavepoint(HOST, PORT, JOB_ID, JOB_NAME, TARGET_DIR, false, 1L));
    }

    @Test
    void triggerSavepoint_pollsUntilCompleted_afterInProgressResponse() throws Exception {
        when(restClient.postSavepointRequest(HOST, PORT, JOB_ID, TARGET_DIR, false))
                .thenReturn("req-005");
        when(restClient.getSavepointStatus(HOST, PORT, JOB_ID, "req-005"))
                .thenReturn(new FlinkRestClient.SavepointStatusResponse("IN_PROGRESS", null))
                .thenReturn(new FlinkRestClient.SavepointStatusResponse("IN_PROGRESS", null))
                .thenReturn(new FlinkRestClient.SavepointStatusResponse("COMPLETED",
                        "file:///app/checkpoints/savepoints/savepoint-final"));

        SavepointRecord record = service.triggerSavepoint(
                HOST, PORT, JOB_ID, JOB_NAME, TARGET_DIR, false, TIMEOUT_MS);

        assertEquals("file:///app/checkpoints/savepoints/savepoint-final", record.getSavepointPath());
        verify(restClient, times(3)).getSavepointStatus(HOST, PORT, JOB_ID, "req-005");
    }
}
