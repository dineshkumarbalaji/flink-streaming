package com.antheon.flink.streaming.source;

import com.antheon.flink.streaming.config.AuthConfig;
import com.antheon.flink.streaming.config.KafkaConfig;
import com.antheon.flink.streaming.config.SourceConfig;
import com.antheon.flink.streaming.config.WatermarkConfig;
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
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;

@Slf4j
@Component
public class KafkaSourceLayer {
    
    // ... (keep createSourceTable signature)
    public Table createSourceTable(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            SourceConfig sourceConfig) {
        
        log.info("Creating Kafka source for topic: {}, Format: {}", sourceConfig.getKafka().getTopic(), sourceConfig.getKafka().getFormat());
        
        // Build Kafka properties
        Properties kafkaProps = buildKafkaProperties(sourceConfig.getKafka());
        
        // Determine Offset Strategy
        OffsetsInitializer offsetsInitializer = OffsetsInitializer.earliest();
        String offsetStrategy = sourceConfig.getKafka().getStartingOffset();
        
        if (offsetStrategy != null) {
            switch (offsetStrategy) {
                case "LATEST":
                    offsetsInitializer = OffsetsInitializer.latest();
                    break;
                case "GROUP_OFFSETS":
                    offsetsInitializer = OffsetsInitializer.committedOffsets(OffsetsInitializer.earliest().getAutoOffsetResetStrategy());
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
                .fromSource(kafkaSource, watermarkStrategy, "Kafka Source")
                .uid("kafka-source-" + sourceConfig.getKafka().getTopic());
        
        Table sourceTable;
        String format = sourceConfig.getKafka().getFormat();
        
        // Handle Schema Validation: Apply if format is JSON/AVRO and Schema is present
        boolean useValidation = (sourceConfig.getSchema() != null && !sourceConfig.getSchema().isEmpty());
        
        if (useValidation) {
            log.info("Using schema validation: {} for format {}", sourceConfig.getSchema(), format);
            
            SingleOutputStreamOperator<Row> validatedStream;
            List<FieldDefinition> fields;
            
            if ("AVRO".equalsIgnoreCase(format)) {
                 fields = parseAvroSchemaString(sourceConfig.getSchema());
                 RowTypeInfo rowTypeInfo = createRowTypeInfo(fields);
                 
                 validatedStream = rawStream
                    .flatMap(new AvroSchemaValidator(fields, sourceConfig.getSchema()))
                    .returns(rowTypeInfo)
                    .uid("avro-schema-validator");
            
            } else {
                 fields = parseSchemaString(sourceConfig.getSchema());
                 RowTypeInfo rowTypeInfo = createRowTypeInfo(fields);
                 
                 validatedStream = rawStream
                    .flatMap(new SchemaValidator(fields, sourceConfig.getSchema()))
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
        } else {
            
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
        } else if ("EXISTING".equalsIgnoreCase(mode) && config.getWatermark().getTimestampColumn() != null) {
             String col = config.getWatermark().getTimestampColumn();
             builder.watermark(col, col + " - INTERVAL '5' SECOND");
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
        // STUBBED
        List<FieldDefinition> fields = new ArrayList<>();
        // Just return empty or dummy for test
        // Real parsing commented out
        
        // Minimal logic to allow "Validation" path to run without crashing if schema present:
        // fields.add(new FieldDefinition("dummy", "STRING"));
        // BUT better to just keep the parsing logic that uses Jackson since Jackson is safe?
        // Let's comment out the risky parts only.
        
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
                    
                    String type = "STRING"; // Default
                    if (typeNode != null) {
                        String jsonType = typeNode.asText().toLowerCase();
                        switch (jsonType) {
                            case "integer": type = "INT"; break;
                            case "number": type = "DOUBLE"; break;
                            case "boolean": type = "BOOLEAN"; break;
                            case "string": type = "STRING"; break;
                            default: type = "STRING";
                        }
                    }
                    fields.add(new FieldDefinition(fieldName, type));
                }
            }
        } catch (Exception e) {
            log.error("Failed to parse JSON Schema", e);
        }
        return fields;
    }
    
    private List<FieldDefinition> parseAvroSchemaString(String schemaStr) {
        List<FieldDefinition> fields = new ArrayList<>();
        try {
            org.apache.avro.Schema schema = new Parser().parse(schemaStr);
            for (org.apache.avro.Schema.Field field : schema.getFields()) {
                 String type = "STRING";
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
                     case INT: type = "INT"; break;
                     case DOUBLE: type = "DOUBLE"; break;
                     case BOOLEAN: type = "BOOLEAN"; break;
                     case LONG: type = "INT"; break; // Map LONG to INT for now OR STRING? Using INT as per definitions
                     case FLOAT: type = "DOUBLE"; break;
                     default: type = "STRING";
                 }
                 fields.add(new FieldDefinition(field.name(), type));
            }
        } catch (Exception e) {
            log.error("Failed to parse Avro Schema", e);
        }
        return fields;
    }

    private RowTypeInfo createRowTypeInfo(List<FieldDefinition> fields) {
        TypeInformation<?>[] types = new TypeInformation[fields.size()];
        String[] names = new String[fields.size()];
        
        for (int i = 0; i < fields.size(); i++) {
            names[i] = fields.get(i).name;
            switch (fields.get(i).type) {
                case "INT":
                    types[i] = Types.INT;
                    break;
                case "DOUBLE":
                    types[i] = Types.DOUBLE;
                    break;
                case "BOOLEAN":
                    types[i] = Types.BOOLEAN;
                    break;
                default:
                    types[i] = Types.STRING;
            }
        }
        return new RowTypeInfo(types, names);
    }
    
    private org.apache.flink.table.types.DataType mapToDataType(String type) {
        switch (type) {
            case "INT": return DataTypes.INT();
            case "DOUBLE": return DataTypes.DOUBLE();
            case "BOOLEAN": return DataTypes.BOOLEAN();
            default: return DataTypes.STRING();
        }
    }

    public static class SchemaValidator extends RichFlatMapFunction<String, Row> {
        private final List<FieldDefinition> fields;
        private final String schemaStr;
        
        private transient ObjectMapper objectMapper;
        private transient JsonSchema jsonSchema; 
        private transient org.apache.flink.metrics.Counter counter;

        public SchemaValidator(List<FieldDefinition> fields, String schemaStr) {
            this.fields = fields;
            this.schemaStr = schemaStr;
        }

        public SchemaValidator(List<FieldDefinition> fields) {
           this(fields, null);
        }

        public void open(Configuration parameters) {
            this.counter = getRuntimeContext().getMetricGroup().counter("records-consumed");
            objectMapper = new ObjectMapper();
            if (schemaStr != null) {
                try {
                    JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V201909);
                    jsonSchema = factory.getSchema(schemaStr);
                } catch(Exception e) {
                    System.err.println("Failed to init JSON schema: " + e.getMessage());
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
                        System.err.println("Invalid JSON Record: " + value + " Errors: " + errors);
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
                        System.err.println("Type Conversion Error for field " + field.name + ": " + e.getMessage());
                        return;
                    }
                }
                out.collect(row);
                if (this.counter != null) this.counter.inc();
                
            } catch (Exception e) {
                System.err.println("Malformed JSON: " + value);
            }
        }
    }
    
    public static class AvroSchemaValidator extends RichFlatMapFunction<String, Row> {
        private final List<FieldDefinition> fields;
        private final String schemaStr;
        
        private transient org.apache.avro.Schema avroSchema;
        private transient org.apache.flink.metrics.Counter counter;
        private transient DatumReader<GenericRecord> reader;

        public AvroSchemaValidator(List<FieldDefinition> fields, String schemaStr) {
            this.fields = fields;
            this.schemaStr = schemaStr;
        }

        @Override
        public void open(Configuration parameters) {
            this.counter = getRuntimeContext().getMetricGroup().counter("records-consumed");
            if (schemaStr != null) {
                try {
                    this.avroSchema = new Parser().parse(schemaStr);
                    this.reader = new GenericDatumReader<>(avroSchema);
                } catch (Exception e) {
                    System.err.println("Failed to parse Avro Schema: " + e.getMessage());
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
                 if (this.counter != null) this.counter.inc();
                 
            } catch (Exception e) {
                System.err.println("Malformed Avro/JSON Record: " + value + " Error: " + e.getMessage());
            }
        }
    }
    
    // ... keep MetricReportingMapFunction and rest ...

    public static class MetricReportingMapFunction extends org.apache.flink.api.common.functions.RichMapFunction<String, String> {
        private transient org.apache.flink.metrics.Counter counter;

        @Override
        public void open(Configuration parameters) {
            this.counter = getRuntimeContext().getMetricGroup().counter("records-consumed");
        }

        @Override
        public String map(String value) {
            if (this.counter != null) {
                this.counter.inc();
            }
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
    
    private WatermarkStrategy<String> createWatermarkStrategy(WatermarkConfig config) {
        if (config == null || "NONE".equalsIgnoreCase(config.getStrategy())) {
            return WatermarkStrategy.noWatermarks();
        }
        
        if ("BOUNDED".equalsIgnoreCase(config.getStrategy())) {
            return WatermarkStrategy
                    .<String>forBoundedOutOfOrderness(
                            java.time.Duration.ofMillis(config.getMaxOutOfOrderness())
                     )
                    .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis());
        }
        
        return WatermarkStrategy.noWatermarks();
    }
}