package com.datahondo.flink.streaming.savepoint;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SavepointRegistryTest {

    private SavepointRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SavepointRegistry();
    }

    @Test
    void register_andGetForJob_returnsRegisteredRecord() {
        SavepointRecord record = makeRecord("job1", "file:///sp/sp-001");
        registry.register("job1", record);

        List<SavepointRecord> results = registry.getForJob("job1");
        assertEquals(1, results.size());
        assertEquals("file:///sp/sp-001", results.get(0).getSavepointPath());
    }

    @Test
    void getForJob_returnsEmptyList_whenJobHasNoSavepoints() {
        assertTrue(registry.getForJob("no-such-job").isEmpty());
    }

    @Test
    void register_multipleRecords_returnsAllInInsertionOrder() {
        registry.register("job1", makeRecord("job1", "file:///sp/sp-001"));
        registry.register("job1", makeRecord("job1", "file:///sp/sp-002"));
        registry.register("job1", makeRecord("job1", "file:///sp/sp-003"));

        List<SavepointRecord> results = registry.getForJob("job1");
        assertEquals(3, results.size());
        assertEquals("file:///sp/sp-001", results.get(0).getSavepointPath());
        assertEquals("file:///sp/sp-003", results.get(2).getSavepointPath());
    }

    @Test
    void register_isolatesRecordsByJobName() {
        registry.register("job1", makeRecord("job1", "file:///sp/sp-001"));
        registry.register("job2", makeRecord("job2", "file:///sp/sp-002"));

        assertEquals(1, registry.getForJob("job1").size());
        assertEquals(1, registry.getForJob("job2").size());
        assertEquals("file:///sp/sp-001", registry.getForJob("job1").get(0).getSavepointPath());
    }

    @Test
    void getLatest_returnsLastRegistered() {
        registry.register("job1", makeRecord("job1", "file:///sp/sp-001"));
        registry.register("job1", makeRecord("job1", "file:///sp/sp-002"));

        SavepointRecord latest = registry.getLatest("job1");
        assertNotNull(latest);
        assertEquals("file:///sp/sp-002", latest.getSavepointPath());
    }

    @Test
    void getLatest_returnsNull_whenNoSavepointsExist() {
        assertNull(registry.getLatest("ghost-job"));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private SavepointRecord makeRecord(String jobName, String path) {
        return SavepointRecord.builder()
                .jobName(jobName)
                .jobId("flink-job-id-" + jobName)
                .savepointPath(path)
                .createdAt(Instant.now())
                .cancelledJob(false)
                .build();
    }
}
