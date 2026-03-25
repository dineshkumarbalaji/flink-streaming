package com.datahondo.flink.streaming.sink;

import com.datahondo.flink.streaming.config.AuthConfig;
import com.datahondo.flink.streaming.config.KafkaConfig;
import com.datahondo.flink.streaming.config.TargetConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
public class KafkaTargetLayer {

    public void sinkToKafka(StreamTableEnvironment tableEnv, Table transformedTable, TargetConfig config) {
        log.info("Creating Kafka sink for topic: {}, Format: {}",
                config.getKafka().getTopic(), config.getKafka().getFormat());

        // Capture column names from the resolved schema before converting to DataStream
        ResolvedSchema schema = transformedTable.getResolvedSchema();
        List<String> columnNames = schema.getColumnNames();
        String[] fieldNames = columnNames.toArray(new String[0]);

        // Convert Table to DataStream<Row>
        DataStream<Row> rowStream = tableEnv.toDataStream(transformedTable);

        // Serialize rows to strings based on output format
        String format = config.getKafka().getFormat();
        DataStream<String> outputStream;

        if ("JSON".equalsIgnoreCase(format)) {
            outputStream = rowStream
                    .map(new RowToJsonMapper(fieldNames))
                    .uid("row-to-json-" + config.getKafka().getTopic());
        } else {
            // STRING or default
            outputStream = rowStream
                    .map(new RowToStringMapper())
                    .uid("row-to-string-" + config.getKafka().getTopic());
        }

        // Build Kafka producer properties (auth + custom)
        Properties kafkaProps = buildKafkaProperties(config.getKafka());

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(config.getKafka().getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.getKafka().getTopic())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setKafkaProducerConfig(kafkaProps)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        outputStream
                .sinkTo(sink)
                .uid("kafka-sink-" + config.getKafka().getTopic())
                .name("Kafka Sink: " + config.getKafka().getTopic());

        log.info("Kafka sink '{}' registered successfully", config.getKafka().getTopic());
    }

    private Properties buildKafkaProperties(KafkaConfig kafkaConfig) {
        Properties props = new Properties();

        if (kafkaConfig.getProperties() != null) {
            props.putAll(kafkaConfig.getProperties());
        }

        if (kafkaConfig.getAuthentication() != null) {
            AuthConfig auth = kafkaConfig.getAuthentication();

            if ("SASL_SSL".equalsIgnoreCase(auth.getType()) ||
                    "SASL_PLAINTEXT".equalsIgnoreCase(auth.getType())) {

                props.put("security.protocol", auth.getType());
                props.put("sasl.mechanism", auth.getMechanism());

                String loginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
                if ("SCRAM-SHA-256".equalsIgnoreCase(auth.getMechanism())) {
                    loginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
                }

                String jaasConfig = String.format(
                        "%s required username=\"%s\" password=\"%s\";",
                        loginModule, auth.getUsername(), auth.getPassword()
                );
                props.put("sasl.jaas.config", jaasConfig);
            }
        }

        return props;
    }

    public static class RowToJsonMapper extends RichMapFunction<Row, String> {
        private final String[] fieldNames;
        private transient ObjectMapper objectMapper;

        public RowToJsonMapper(String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        @Override
        public void open(Configuration parameters) {
            this.objectMapper = new ObjectMapper();
        }

        @Override
        public String map(Row row) throws Exception {
            Map<String, Object> result = new LinkedHashMap<>();
            for (int i = 0; i < fieldNames.length; i++) {
                result.put(fieldNames[i], row.getField(i));
            }
            return objectMapper.writeValueAsString(result);
        }
    }

    public static class RowToStringMapper extends RichMapFunction<Row, String> {
        @Override
        public String map(Row row) {
            return row.toString();
        }
    }
}
