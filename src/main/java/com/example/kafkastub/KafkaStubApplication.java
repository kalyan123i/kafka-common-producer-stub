package com.example.kafkastub;

import com.example.kafkastub.config.AppProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
public class KafkaStubApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaStubApplication.class, args);
    }
}
