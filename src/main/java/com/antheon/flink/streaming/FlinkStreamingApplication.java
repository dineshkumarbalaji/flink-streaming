package com.antheon.flink.streaming;


import com.antheon.flink.streaming.config.StreamingJobConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(StreamingJobConfig.class)
public class FlinkStreamingApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlinkStreamingApplication.class, args);
    }
}