package com.datahondo.flink.streaming.target;

import com.datahondo.flink.streaming.audit.AuditAccumulators;
import com.datahondo.flink.streaming.audit.AuditCountingMapFunction;
import com.datahondo.flink.streaming.config.AuthConfig;
import com.datahondo.flink.streaming.config.KafkaConfig;
import com.datahondo.flink.streaming.config.TargetConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Properties;
import java.io.ByteArrayOutputStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumWriter;

@Slf4j
@Component
public class KafkaTargetLayer implements TargetLayer {

    private static final String FORMAT_JSON = "JSON";
    private static final String FORMAT_AVRO = "AVRO";

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Override
    public String getSinkType() {
        return "KAFKA";
    }

    @Override
    public void sink(
            StreamTableEnvironment tableEnv,
            Table resultTable,
            TargetConfig targetConfig) {
        
        log.info("Creating Kafka sink for topic: {}, Format: {}", targetConfig.getKafka().getTopic(), targetConfig.getKafka().getFormat());
        
        // Build Kafka properties
        Properties kafkaProps = buildKafkaProperties(targetConfig.getKafka());
        
        String format = targetConfig.getKafka().getFormat();
        
        // Use byte[] sink to support all formats (String, JSON, Avro)
        KafkaSink<byte[]> kafkaSink = KafkaSink.<byte[]>builder()
                .setBootstrapServers(targetConfig.getKafka().getBootstrapServers())
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.<byte[]>builder()
                        .setTopic(targetConfig.getKafka().getTopic())
                        .setValueSerializationSchema(new org.apache.flink.api.common.serialization.SerializationSchema<byte[]>() {
                            @Override
                            public byte[] serialize(byte[] element) {
                                return element;
                            }
                        })
                        .build()
                )
                .setKafkaProducerConfig(kafkaProps)
                .build();
                
        // ── Transformation boundary ───────────────────────────────────────────
        // startNewChain() breaks the chain from Table API operators so this
        // appears as a distinct "Transformation" node in the Flink job graph.
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable)
                .map(new AuditCountingMapFunction(AuditAccumulators.TRANSFORM_OUT))
                .startNewChain()
                .name("Transformation")
                .uid("audit-transform-out");

        String targetTopic = targetConfig.getKafka().getTopic();

        // ── Sink boundary ─────────────────────────────────────────────────────
        // startNewChain() on the serializer breaks it from the Transformation node
        // so the job graph shows: Source → Transformation → Sink
        SingleOutputStreamOperator<byte[]> serializedStream;

        if (FORMAT_JSON.equalsIgnoreCase(format)) {
            serializedStream = resultStream.map(row -> {
                java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
                java.util.Set<String> nameSet = row.getFieldNames(true);
                String[] names = (nameSet != null) ? nameSet.toArray(new String[0]) : new String[0];
                for (int i = 0; i < row.getArity(); i++) {
                    String key = (i < names.length) ? names[i] : "f" + i;
                    map.put(key, row.getField(i));
                }
                return JSON_MAPPER.writeValueAsBytes(map);
            }).startNewChain()
              .name("Sink: Serializer (JSON) [" + targetTopic + "]")
              .uid("json-serializer");

        } else if (FORMAT_AVRO.equalsIgnoreCase(format)) {
            String schemaStr = (targetConfig.getSchema() != null) ? targetConfig.getSchema().getDefinition() : null;
            if (schemaStr != null && !schemaStr.isEmpty()) {
                serializedStream = resultStream
                        .map(new AvroRowSerializer(schemaStr, targetTopic))
                        .startNewChain()
                        .name("Sink: Serializer (AVRO) [" + targetTopic + "]")
                        .uid("avro-serializer");
            } else {
                log.warn("AVRO target format selected but no schema provided. Falling back to String bytes.");
                serializedStream = resultStream
                        .map(row -> row.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        .startNewChain()
                        .name("Sink: Serializer (STRING fallback) [" + targetTopic + "]")
                        .uid("avro-serializer-fallback");
            }
        } else {
            serializedStream = resultStream
                    .map(row -> row.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8))
                    .startNewChain()
                    .name("Sink: Serializer (STRING) [" + targetTopic + "]")
                    .uid("string-serializer");
        }

        serializedStream
                .sinkTo(kafkaSink)
                .uid("kafka-sink-" + targetConfig.getKafka().getTopic())
                .name("Kafka Sink");
        
        log.info("Kafka sink configured successfully");
    }
    
    // Inner class for Avro Serialization
    public static class AvroRowSerializer extends org.apache.flink.api.common.functions.RichMapFunction<Row, byte[]> {
        private final String schemaStr;
        private final String topicName;
        private transient Schema schema;
        private transient GenericDatumWriter<GenericRecord> writer;
        private transient LongCounter writtenCounter;

        public AvroRowSerializer(String schemaStr, String topicName) {
            this.schemaStr = schemaStr;
            this.topicName = topicName;
        }

        /** Backwards-compatible single-arg constructor (topic defaults to "unknown"). */
        public AvroRowSerializer(String schemaStr) {
            this(schemaStr, "unknown");
        }

        @Override
        public void open(Configuration parameters) {
            try {
                this.schema = new Schema.Parser().parse(schemaStr);
                this.writer = new GenericDatumWriter<>(schema);
                this.writtenCounter = getRuntimeContext()
                        .getLongCounter(AuditAccumulators.targetWritten(topicName));
            } catch (Exception e) {
                log.error("Failed to parse Avro schema", e);
                throw new RuntimeException("Invalid Avro Schema", e);
            }
        }
        
        @Override
        public byte[] map(Row row) throws Exception {
            if (schema == null) return new byte[0];
            
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                GenericRecord record = new GenericData.Record(schema);
                // Map Row fields to Avro fields by index? 
                // Or by name if Row has headers (Table API result usually keeps names in schema but Row might lose them depending on conversion).
                // resultTable.toDataStream() produces Row. 
                // We assume order matches.
                
                int i = 0;
                for (Schema.Field field : schema.getFields()) {
                    if (i >= row.getArity()) break;
                    Object val = row.getField(i);
                    // Basic type conversion might be needed here. 
                    // Avro is picky. Integer vs int, etc.
                    // For now, pass raw and hope for best or toString it if String type.
                    if (val != null) {
                        // Handle Type Conversions for Avro
                        if (val instanceof java.time.Instant) {
                            val = ((java.time.Instant) val).toEpochMilli();
                        } else if (val instanceof java.time.LocalDateTime) {
                            // Flink LocalDateTime is often treated as local. 
                            // Converting to Instant requires Zone. We'll use system default or UTC.
                            val = ((java.time.LocalDateTime) val).atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                        } else if (val instanceof java.sql.Timestamp) {
                            val = ((java.sql.Timestamp) val).getTime();
                        } else if (val instanceof CharSequence) {
                            val = val.toString();
                        }
                        
                        record.put(field.name(), val);
                    }
                    i++;
                }
                
                Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                writer.write(record, encoder);
                encoder.flush();
                writtenCounter.add(1L);
                return out.toByteArray();
                
            } catch (Exception e) {
                log.error("Avro serialization error: " + e.getMessage());
                // Fallback or rethrow? rethrow to fail job explicitly if requested?
                // Or return empty to drop.
                throw e; 
            }
        }
    }

    private Properties buildKafkaProperties(KafkaConfig kafkaConfig) {
        Properties props = new Properties();
        
        // Add custom properties
        if (kafkaConfig.getProperties() != null) {
            props.putAll(kafkaConfig.getProperties());
        }
        
        // Add authentication if configured
        if (kafkaConfig.getAuthentication() != null) {
            AuthConfig auth = kafkaConfig.getAuthentication();
            
            if ("SASL_SSL".equalsIgnoreCase(auth.getType()) ||
                "SASL_PLAINTEXT".equalsIgnoreCase(auth.getType())) {

                props.put("security.protocol", auth.getType());
                props.put("sasl.mechanism", auth.getMechanism());

                // Use explicit jaasConfig override if provided, otherwise build from credentials
                if (auth.getJaasConfig() != null && !auth.getJaasConfig().isEmpty()) {
                    props.put("sasl.jaas.config", auth.getJaasConfig());
                } else {
                    String loginModule = "SCRAM-SHA-256".equalsIgnoreCase(auth.getMechanism())
                        ? "org.apache.kafka.common.security.scram.ScramLoginModule"
                        : "org.apache.kafka.common.security.plain.PlainLoginModule";
                    props.put("sasl.jaas.config", String.format(
                        "%s required username=\"%s\" password=\"%s\";",
                        loginModule, auth.getUsername(), auth.getPassword()
                    ));
                }

                // SSL truststore (required for SASL_SSL)
                if ("SASL_SSL".equalsIgnoreCase(auth.getType())) {
                    if (auth.getTruststoreLocation() != null && !auth.getTruststoreLocation().isEmpty()) {
                        props.put("ssl.truststore.location", auth.getTruststoreLocation());
                        if (auth.getTruststorePassword() != null) {
                            props.put("ssl.truststore.password", auth.getTruststorePassword());
                        }
                    } else {
                        log.warn("SASL_SSL configured but truststoreLocation is not set");
                    }
                }
            }
        }

        return props;
    }
}
