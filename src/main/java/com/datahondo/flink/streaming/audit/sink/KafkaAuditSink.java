package com.datahondo.flink.streaming.audit.sink;

import com.datahondo.flink.streaming.audit.AuditEvent;
import com.datahondo.flink.streaming.audit.AuditSink;
import com.datahondo.flink.streaming.config.AuthConfig;
import com.datahondo.flink.streaming.config.AuditConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * {@link AuditSink} that publishes each {@link AuditEvent} as a JSON message
 * to a dedicated Kafka topic.
 *
 * <p>The message key is {@code runId} so that all events for a job execution
 * land in the same partition when a key-based partitioner is in use.
 *
 * <p>The producer is created lazily on the first {@link #emit(AuditEvent)} call
 * and is intentionally never closed (JVM-lifecycle singleton).  For higher-volume
 * deployments consider batching or an async linger.
 *
 * <p><strong>Current status: PRODUCTION-READY STUB.</strong>
 * The implementation is functionally complete but Schema Registry integration
 * and exactly-once semantics are deferred to a future iteration.
 */
@Slf4j
public class KafkaAuditSink implements AuditSink {

    private final AuditConfig config;
    private final ObjectMapper mapper;
    private volatile KafkaProducer<String, String> producer;

    public KafkaAuditSink(AuditConfig config) {
        this.config = config;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void emit(AuditEvent event) {
        try {
            String json = mapper.writeValueAsString(event);
            getProducer().send(
                    new ProducerRecord<>(config.getDestination(), event.getRunId(), json),
                    (metadata, ex) -> {
                        if (ex != null) {
                            log.warn("[AUDIT-KAFKA] Failed to deliver audit event: runId={}, error={}",
                                    event.getRunId(), ex.getMessage());
                        }
                    });
        } catch (Exception e) {
            log.warn("[AUDIT-KAFKA] Serialisation error for runId={}: {}",
                    event.getRunId(), e.getMessage());
        }
    }

    @Override
    public String sinkType() {
        return "KAFKA";
    }

    // ── Lazy producer init ────────────────────────────────────────────────────

    private KafkaProducer<String, String> getProducer() {
        if (producer == null) {
            synchronized (this) {
                if (producer == null) {
                    producer = new KafkaProducer<>(buildProducerProps());
                    log.info("[AUDIT-KAFKA] Producer initialised for topic={}",
                            config.getDestination());
                }
            }
        }
        return producer;
    }

    private Properties buildProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getConnection());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");           // at-least-once
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);       // minor batching
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        if (config.getKafkaAuth() != null) {
            AuthConfig auth = config.getKafkaAuth();
            if (auth.getType() != null) {
                props.put("security.protocol", auth.getType());
                props.put("sasl.mechanism", auth.getMechanism());
                if (auth.getJaasConfig() != null && !auth.getJaasConfig().isEmpty()) {
                    props.put("sasl.jaas.config", auth.getJaasConfig());
                } else {
                    String module = "SCRAM-SHA-256".equalsIgnoreCase(auth.getMechanism())
                            ? "org.apache.kafka.common.security.scram.ScramLoginModule"
                            : "org.apache.kafka.common.security.plain.PlainLoginModule";
                    props.put("sasl.jaas.config", String.format(
                            "%s required username=\"%s\" password=\"%s\";",
                            module, auth.getUsername(), auth.getPassword()));
                }
            }
        }
        return props;
    }
}
