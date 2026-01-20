package com.antheon.flink.streaming.web.service;

import com.antheon.flink.streaming.config.AuthConfig;
import com.antheon.flink.streaming.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class KafkaValidatorService {

    public void validateConnection(KafkaConfig config) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000); // 5 seconds timeout
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);

        // Add Authentication
        if (config.getAuthentication() != null) {
            AuthConfig auth = config.getAuthentication();
            if ("SASL_SSL".equalsIgnoreCase(auth.getType()) || "SASL_PLAINTEXT".equalsIgnoreCase(auth.getType())) {
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

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Test listing topics to verify connection and auth
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> topics = listTopics.names().get(5, TimeUnit.SECONDS);

            // Verify topic exists
            if (!topics.contains(config.getTopic())) {
                throw new Exception("Topic '" + config.getTopic() + "' not found or not accessible.");
            }
        } catch (Exception e) {
            log.error("Validation failed for Kafka config: {}", config.getBootstrapServers(), e);
            throw new Exception("Connection failed: " + e.getMessage());
        }
    }
}
