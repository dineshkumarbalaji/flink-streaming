package com.datahondo.flink.streaming.savepoint;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Thread-safe registry of {@link SavepointRecord} entries per job.
 *
 * <p>Records are persisted to {@code <configDir>/<jobName>-savepoints.json} on every
 * {@link #register(String, SavepointRecord)} call and loaded from disk on startup, so
 * savepoint history survives application restarts.
 */
@Slf4j
@Component
public class SavepointRegistry {

    private final Map<String, CopyOnWriteArrayList<SavepointRecord>> store = new ConcurrentHashMap<>();

    @Value("${streaming.job.flink.config-dir:configs}")
    private String configDir;

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @PostConstruct
    public void loadFromDisk() {
        File dir = new File(configDir);
        if (!dir.exists()) return;
        File[] files = dir.listFiles((d, name) -> name.endsWith("-savepoints.json"));
        if (files == null) return;
        for (File f : files) {
            try {
                List<SavepointRecord> records = mapper.readValue(f,
                        new TypeReference<List<SavepointRecord>>() {});
                String jobName = f.getName().replace("-savepoints.json", "");
                store.put(jobName, new CopyOnWriteArrayList<>(records));
                log.info("[SAVEPOINT-REGISTRY] Loaded {} savepoint(s) for job '{}'",
                        records.size(), jobName);
            } catch (Exception e) {
                log.warn("[SAVEPOINT-REGISTRY] Failed to load {}: {}", f.getName(), e.getMessage());
            }
        }
    }

    public void register(String jobName, SavepointRecord record) {
        if (jobName == null || record == null) return;
        store.computeIfAbsent(jobName, k -> new CopyOnWriteArrayList<>()).add(record);
        persistToDisk(jobName);
    }

    public List<SavepointRecord> getForJob(String jobName) {
        if (jobName == null) return Collections.emptyList();
        CopyOnWriteArrayList<SavepointRecord> list = store.get(jobName);
        return list == null ? Collections.emptyList() : new ArrayList<>(list);
    }

    public SavepointRecord getLatest(String jobName) {
        List<SavepointRecord> records = getForJob(jobName);
        return records.isEmpty() ? null : records.get(records.size() - 1);
    }

    private void persistToDisk(String jobName) {
        try {
            File dir = new File(configDir);
            dir.mkdirs();
            File f = new File(dir, jobName + "-savepoints.json");
            mapper.enable(SerializationFeature.INDENT_OUTPUT).writeValue(f, getForJob(jobName));
            log.debug("[SAVEPOINT-REGISTRY] Persisted savepoints for '{}' → {}", jobName, f.getPath());
        } catch (Exception e) {
            log.warn("[SAVEPOINT-REGISTRY] Failed to persist savepoints for '{}': {}",
                    jobName, e.getMessage());
        }
    }
}
