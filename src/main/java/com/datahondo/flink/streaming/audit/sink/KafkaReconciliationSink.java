package com.datahondo.flink.streaming.audit.sink;

import com.datahondo.flink.streaming.audit.ReconciliationReport;
import com.datahondo.flink.streaming.audit.ReconciliationSink;
import com.datahondo.flink.streaming.config.ReconciliationConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class KafkaReconciliationSink implements ReconciliationSink {

    private final ReconciliationConfig config;
    private final ObjectMapper mapper;
    private volatile KafkaProducer<String, String> producer;

    public KafkaReconciliationSink(ReconciliationConfig config) {
        this.config = config;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void emit(ReconciliationReport report) {
        try {
            String json = mapper.writeValueAsString(report);
            getProducer().send(
                    new ProducerRecord<>(config.getDestination(), report.getRunId(), json),
                    (metadata, ex) -> {
                        if (ex != null) {
                            log.warn("[RECONCILIATION-KAFKA] Failed to deliver report: runId={}, error={}",
                                    report.getRunId(), ex.getMessage());
                        }
                    });
        } catch (Exception e) {
            log.warn("[RECONCILIATION-KAFKA] Serialisation error for runId={}: {}",
                    report.getRunId(), e.getMessage());
        }
    }

    @Override
    public String sinkType() {
        return "KAFKA";
    }

    private KafkaProducer<String, String> getProducer() {
        if (producer == null) {
            synchronized (this) {
                if (producer == null) {
                    producer = new KafkaProducer<>(buildProducerProps());
                    log.info("[RECONCILIATION-KAFKA] Producer initialised for topic={}",
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
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return props;
    }
}
