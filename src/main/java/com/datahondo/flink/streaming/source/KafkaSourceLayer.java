package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.AuthConfig;
import com.datahondo.flink.streaming.config.KafkaConfig;
import com.datahondo.flink.streaming.config.SourceConfig;
import com.datahondo.flink.streaming.config.WatermarkConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import com.datahondo.flink.streaming.audit.AuditAccumulators;
import com.datahondo.flink.streaming.exception.SchemaException;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;

@Slf4j
@Component
public class KafkaSourceLayer implements SourceLayer {

    private static final String TYPE_STRING = "STRING";
    private static final String TYPE_INT = "INT";
    private static final String TYPE_LONG = "LONG";
    private static final String TYPE_DOUBLE = "DOUBLE";
    private static final String TYPE_BOOLEAN = "BOOLEAN";
    private static final String TYPE_TIMESTAMP = "TIMESTAMP";
    
    @Override
    public Table createSourceTable(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            SourceConfig sourceConfig) {
        
        log.info("Creating Kafka source for topic: {}, Format: {}", sourceConfig.getKafka().getTopic(), sourceConfig.getKafka().getFormat());
        
        // Build Kafka properties
        Properties kafkaProps = buildKafkaProperties(sourceConfig.getKafka());
        
        // Determine Offset Strategy — startupMode (Flink-native) takes priority over startingOffset
        OffsetsInitializer offsetsInitializer = OffsetsInitializer.earliest();
        String startupMode = sourceConfig.getKafka().getStartupMode();
        String offsetStrategy = sourceConfig.getKafka().getStartingOffset();

        if (startupMode != null) {
            switch (startupMode.toLowerCase()) {
                case "latest-offset":
                    offsetsInitializer = OffsetsInitializer.latest();
                    break;
                case "group-offsets":
                    offsetsInitializer = OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
                    break;
                case "timestamp":
                    if (sourceConfig.getKafka().getStartingOffsetTimestamp() != null) {
                        offsetsInitializer = OffsetsInitializer.timestamp(sourceConfig.getKafka().getStartingOffsetTimestamp());
                    } else {
                        log.warn("startupMode=timestamp but no startingOffsetTimestamp provided. Defaulting to earliest.");
                    }
                    break;
                case "earliest-offset":
                default:
                    offsetsInitializer = OffsetsInitializer.earliest();
            }
            log.info("Using startupMode '{}' for topic {}", startupMode, sourceConfig.getKafka().getTopic());
        } else if (offsetStrategy != null) {
            switch (offsetStrategy.toUpperCase()) {
                case "LATEST":
                    offsetsInitializer = OffsetsInitializer.latest();
                    break;
                case "GROUP_OFFSETS":
                    offsetsInitializer = OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
                    break;
                case "TIMESTAMP":
                    if (sourceConfig.getKafka().getStartingOffsetTimestamp() != null) {
                        offsetsInitializer = OffsetsInitializer.timestamp(sourceConfig.getKafka().getStartingOffsetTimestamp());
                    } else {
                        log.warn("TIMESTAMP offset strategy selected but no timestamp provided. Defaulting to earliest.");
                    }
                    break;
                case "EARLIEST":
                default:
                    offsetsInitializer = OffsetsInitializer.earliest();
            }
        }

        // Create Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(sourceConfig.getKafka().getBootstrapServers())
                .setTopics(sourceConfig.getKafka().getTopic())
                .setGroupId(sourceConfig.getKafka().getGroupId())
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new SimpleStringSchema()) 
                .setProperties(kafkaProps)
                .build();
        
        // Create data stream with watermark strategy
        WatermarkStrategy<String> watermarkStrategy = createWatermarkStrategy(
                sourceConfig.getWatermark()
        );
        
        DataStream<String> rawStream = env
                .fromSource(kafkaSource, watermarkStrategy,
                        "Source: Kafka [" + sourceConfig.getKafka().getTopic() + "]")
                .uid("kafka-source-" + sourceConfig.getKafka().getTopic());
        
        Table sourceTable;
        String format = sourceConfig.getKafka().getFormat();
        
        // Handle Schema Validation: Apply if format is JSON/AVRO and Schema is present
        boolean useValidation = (sourceConfig.getSchema() != null && sourceConfig.getSchema().hasDefinition());

        if (!useValidation) {

            // Apply Metric Reporting for Raw Stream
            SingleOutputStreamOperator<String> monitoredRawStream = rawStream
                    .map(new MetricReportingMapFunction())
                    .uid("metric-reporter-raw");

            // Default raw behavior
            if (sourceConfig.getWatermark() != null && "BOUNDED".equalsIgnoreCase(sourceConfig.getWatermark().getStrategy())) {
                Schema.Builder schemaBuilder = Schema.newBuilder();
                schemaBuilder.column("f0", DataTypes.STRING());
                applyWatermarkToSchema(schemaBuilder, sourceConfig);
                sourceTable = tableEnv.fromDataStream(monitoredRawStream, schemaBuilder.build());
            } else {
                sourceTable = tableEnv.fromDataStream(monitoredRawStream);
            }
        } else {
            String schemaDefinition = sourceConfig.getSchema().getDefinition();
            log.info("Using schema validation: type={}, format={}", sourceConfig.getSchema().getType(), format);

            SingleOutputStreamOperator<Row> validatedStream;
            List<FieldDefinition> fields;

            if ("AVRO".equalsIgnoreCase(format)) {
                 fields = parseAvroSchemaString(schemaDefinition);
                 RowTypeInfo rowTypeInfo = createRowTypeInfo(fields);

                 validatedStream = rawStream
                    .flatMap(new AvroSchemaValidator(fields, schemaDefinition, sourceConfig.getTableName()))
                    .returns(rowTypeInfo)
                    .uid("avro-schema-validator");

            } else {
                 fields = parseSchemaString(schemaDefinition);
                 RowTypeInfo rowTypeInfo = createRowTypeInfo(fields);

                 validatedStream = rawStream
                    .flatMap(new SchemaValidator(fields, schemaDefinition, sourceConfig.getTableName()))
                    .returns(rowTypeInfo)
                    .uid("schema-validator");
            }

            // Build Schema for Table API
            Schema.Builder schemaBuilder = Schema.newBuilder();
            for (FieldDefinition field : fields) {
                schemaBuilder.column(field.name, mapToDataType(field.type));
            }

            // Apply Watermark
            applyWatermarkToSchema(schemaBuilder, sourceConfig);

            sourceTable = tableEnv.fromDataStream(validatedStream, schemaBuilder.build());
        }

        tableEnv.createTemporaryView(sourceConfig.getTableName(), sourceTable);
        
        log.info("Source table '{}' created successfully", sourceConfig.getTableName());
        return sourceTable;
    }

    private void applyWatermarkToSchema(Schema.Builder builder, SourceConfig config) {
        if (config.getWatermark() == null || !"BOUNDED".equalsIgnoreCase(config.getWatermark().getStrategy())) {
            return;
        }
        String mode = config.getWatermark().getMode();
        if ("PROCESS_TIME".equalsIgnoreCase(mode)) {
            builder.columnByExpression("processed_time", "PROCTIME()");
            // No watermark needed for processing-time — PROCTIME() is not an event-time attribute
        } else if ("EXISTING".equalsIgnoreCase(mode) && config.getWatermark().getTimestampColumn() != null) {
            String col = config.getWatermark().getTimestampColumn();
            long lagSeconds = Math.max(config.getWatermark().getMaxOutOfOrderness() / 1000, 0);
            String watermarkSql = String.format(
                "%s - INTERVAL '%d' SECOND", col, lagSeconds
            );
            log.info("Applying watermark on column '{}' with lag {}s", col, lagSeconds);
            builder.watermark(col, watermarkSql);
        }
    }

    private static class FieldDefinition implements Serializable {
        String name;
        String type; // INT, STRING, BOOLEAN, DOUBLE, LONG

        FieldDefinition(String name, String type) {
            this.name = name;
            this.type = type.trim().toUpperCase();
        }
    }

    private List<FieldDefinition> parseSchemaString(String schemaStr) {
        List<FieldDefinition> fields = new ArrayList<>();
        
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode schemaNode = mapper.readTree(schemaStr);
            JsonNode properties = schemaNode.get("properties");

            if (properties != null && properties.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> it = properties.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    String fieldName = entry.getKey();
                    JsonNode typeNode = entry.getValue().get("type");
                    
                    String type = TYPE_STRING; // Default
                    if (typeNode != null) {
                        String jsonType = typeNode.asText().toLowerCase();
                        switch (jsonType) {
                            case "integer": type = TYPE_INT; break;
                            case "number": type = TYPE_DOUBLE; break;
                            case "boolean": type = TYPE_BOOLEAN; break;
                            case "string": type = TYPE_STRING; break;
                            default: type = TYPE_STRING;
                        }
                    }
                    fields.add(new FieldDefinition(fieldName, type));
                }
            }
        } catch (Exception e) {
            throw new SchemaException("Failed to parse JSON Schema", e);
        }
        return fields;
    }
    
    private List<FieldDefinition> parseAvroSchemaString(String schemaStr) {
        List<FieldDefinition> fields = new ArrayList<>();
        try {
            org.apache.avro.Schema schema = new Parser().parse(schemaStr);
            for (org.apache.avro.Schema.Field field : schema.getFields()) {
                 String type = TYPE_STRING;
                 org.apache.avro.Schema.Type avroType = field.schema().getType();
                 
                 // Handle Union [null, type]
                 if (avroType == org.apache.avro.Schema.Type.UNION) {
                     for (org.apache.avro.Schema s : field.schema().getTypes()) {
                         if (s.getType() != org.apache.avro.Schema.Type.NULL) {
                             avroType = s.getType();
                             break;
                         }
                     }
                 }
                 
                 switch (avroType) {
                     case INT: type = TYPE_INT; break;
                     case LONG: type = TYPE_LONG; break;
                     case DOUBLE:
                     case FLOAT:
                         type = TYPE_DOUBLE; break;
                     case BOOLEAN: type = TYPE_BOOLEAN; break;
                     default: type = TYPE_STRING;
                 }
                 fields.add(new FieldDefinition(field.name(), type));
            }
        } catch (Exception e) {
            throw new SchemaException("Failed to parse Avro Schema", e);
        }
        return fields;
    }

    private RowTypeInfo createRowTypeInfo(List<FieldDefinition> fields) {
        TypeInformation<?>[] types = new TypeInformation[fields.size()];
        String[] names = new String[fields.size()];
        
        for (int i = 0; i < fields.size(); i++) {
            names[i] = fields.get(i).name;
            switch (fields.get(i).type) {
                case TYPE_INT: types[i] = Types.INT; break;
                case TYPE_LONG: types[i] = Types.LONG; break;
                case TYPE_DOUBLE: types[i] = Types.DOUBLE; break;
                case TYPE_BOOLEAN: types[i] = Types.BOOLEAN; break;
                case TYPE_TIMESTAMP: types[i] = Types.SQL_TIMESTAMP; break;
                default: types[i] = Types.STRING;
            }
        }
        return new RowTypeInfo(types, names);
    }
    
    private org.apache.flink.table.types.DataType mapToDataType(String type) {
        switch (type) {
            case TYPE_INT: return DataTypes.INT();
            case TYPE_LONG: return DataTypes.BIGINT();
            case TYPE_DOUBLE: return DataTypes.DOUBLE();
            case TYPE_BOOLEAN: return DataTypes.BOOLEAN();
            case TYPE_TIMESTAMP: return DataTypes.TIMESTAMP(3);
            default: return DataTypes.STRING();
        }
    }

    public static class SchemaValidator extends RichFlatMapFunction<String, Row> {
        private final List<FieldDefinition> fields;
        private final String schemaStr;
        private final String tableName;

        private transient ObjectMapper objectMapper;
        private transient JsonSchema jsonSchema;
        private transient LongCounter readCounter;
        private transient LongCounter rejectedCounter;

        public SchemaValidator(List<FieldDefinition> fields, String schemaStr, String tableName) {
            this.fields = fields;
            this.schemaStr = schemaStr;
            this.tableName = tableName;
        }

        public SchemaValidator(List<FieldDefinition> fields, String schemaStr) {
            this(fields, schemaStr, "unknown");
        }

        public SchemaValidator(List<FieldDefinition> fields) {
            this(fields, null, "unknown");
        }

        public void open(Configuration parameters) {
            readCounter     = getRuntimeContext().getLongCounter(AuditAccumulators.sourceRead(tableName));
            rejectedCounter = getRuntimeContext().getLongCounter(AuditAccumulators.sourceRejected(tableName));
            objectMapper = new ObjectMapper();
            if (schemaStr != null) {
                try {
                    JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V201909);
                    jsonSchema = factory.getSchema(schemaStr);
                } catch(Exception e) {
                    log.error("Failed to init JSON schema: {}", e.getMessage());
                }
            }
        }

        @Override
        public void flatMap(String value, Collector<Row> out) {
            try {
                JsonNode node = objectMapper.readTree(value);
                
                // 1. Validate against JSON Schema
                if (jsonSchema != null) {
                    Set<ValidationMessage> errors = jsonSchema.validate(node);
                    if (!errors.isEmpty()) {
                        log.error("Invalid JSON Record: {} Errors: {}", value, errors);
                        rejectedCounter.add(1L);
                        return;
                    }
                }
                
                // 2. Map to Row
                Row row = Row.withPositions(fields.size());
                for (int i = 0; i < fields.size(); i++) {
                    FieldDefinition field = fields.get(i);
                    JsonNode fieldNode = node.get(field.name);

                    if (fieldNode == null || fieldNode.isNull()) {
                        row.setField(i, null);
                        continue; 
                    }

                    try {
                        switch (field.type) {
                            case "INT":
                                row.setField(i, fieldNode.asInt());
                                break;
                            case "DOUBLE":
                                row.setField(i, fieldNode.asDouble());
                                break;
                            case "BOOLEAN":
                                row.setField(i, fieldNode.asBoolean());
                                break;
                            default:
                                row.setField(i, fieldNode.asText());
                        }
                    } catch (Exception e) {
                        log.error("Type Conversion Error for field {}: {}", field.name, e.getMessage());
                        rejectedCounter.add(1L);
                        return;
                    }
                }
                out.collect(row);
                readCounter.add(1L);

            } catch (Exception e) {
                log.error("Malformed JSON: {}", value);
                rejectedCounter.add(1L);
            }
        }
    }
    
    public static class AvroSchemaValidator extends RichFlatMapFunction<String, Row> {
        private final List<FieldDefinition> fields;
        private final String schemaStr;
        private final String tableName;

        private transient org.apache.avro.Schema avroSchema;
        private transient LongCounter readCounter;
        private transient LongCounter rejectedCounter;
        private transient DatumReader<GenericRecord> reader;

        public AvroSchemaValidator(List<FieldDefinition> fields, String schemaStr, String tableName) {
            this.fields = fields;
            this.schemaStr = schemaStr;
            this.tableName = tableName;
        }

        public AvroSchemaValidator(List<FieldDefinition> fields, String schemaStr) {
            this(fields, schemaStr, "unknown");
        }

        @Override
        public void open(Configuration parameters) {
            readCounter     = getRuntimeContext().getLongCounter(AuditAccumulators.sourceRead(tableName));
            rejectedCounter = getRuntimeContext().getLongCounter(AuditAccumulators.sourceRejected(tableName));
            if (schemaStr != null) {
                try {
                    this.avroSchema = new Parser().parse(schemaStr);
                    this.reader = new GenericDatumReader<>(avroSchema);
                } catch (Exception e) {
                    log.error("Failed to parse Avro Schema: {}", e.getMessage());
                }
            }
        }

        @Override
        public void flatMap(String value, Collector<Row> out) {
            if (avroSchema == null) return;

            try {
                 // Assume JSON-Encoded Avro for String Source
                 Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, value);
                 GenericRecord record = reader.read(null, decoder);
                 
                 Row row = Row.withPositions(fields.size());
                 for (int i = 0; i < fields.size(); i++) {
                     FieldDefinition field = fields.get(i);
                     Object val = record.get(field.name);
                     
                     if (val == null) {
                         row.setField(i, null);
                         continue;
                     }
                     
                     // Avro types might need conversion (e.g. Utf8 to String)
                     // Simple mapping:
                     if (val instanceof CharSequence) {
                         val = val.toString();
                     }
                     row.setField(i, val);
                 }
                 out.collect(row);
                 readCounter.add(1L);

            } catch (Exception e) {
                log.error("Malformed Avro/JSON Record: {} Error: {}", value, e.getMessage());
                rejectedCounter.add(1L);
            }
        }
    }
    
    // ... keep MetricReportingMapFunction and rest ...

    public static class MetricReportingMapFunction extends org.apache.flink.api.common.functions.RichMapFunction<String, String> {
        // Metric counter for Flink UI visibility (no-schema path)
        private transient org.apache.flink.metrics.Counter metricCounter;
        // Accumulator for orchestrator-side reconciliation reads
        private transient LongCounter readCounter;

        @Override
        public void open(Configuration parameters) {
            metricCounter = getRuntimeContext().getMetricGroup().counter("records-consumed");
            readCounter   = getRuntimeContext().getLongCounter(AuditAccumulators.SOURCE_READ_ALL);
        }

        @Override
        public String map(String value) {
            metricCounter.inc();
            readCounter.add(1L);
            return value;
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
    
    private WatermarkStrategy<String> createWatermarkStrategy(WatermarkConfig config) {
        if (config == null || "NONE".equalsIgnoreCase(config.getStrategy())) {
            return WatermarkStrategy.noWatermarks();
        }

        if ("BOUNDED".equalsIgnoreCase(config.getStrategy())) {
            // PROCESS_TIME: assign ingestion time at stream level so PROCTIME() works in Table API.
            // EXISTING: watermark comes from an event-time column declared in the Table schema via
            // applyWatermarkToSchema(); the stream layer must not override the timestamp with
            // System.currentTimeMillis(), so we emit no watermarks here.
            if ("PROCESS_TIME".equalsIgnoreCase(config.getMode())) {
                return WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(
                                java.time.Duration.ofMillis(config.getMaxOutOfOrderness()))
                        .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis());
            }
            // EXISTING mode — Table schema watermark handles this; no stream-level timestamp assignment
            return WatermarkStrategy.noWatermarks();
        }

        return WatermarkStrategy.noWatermarks();
    }
}