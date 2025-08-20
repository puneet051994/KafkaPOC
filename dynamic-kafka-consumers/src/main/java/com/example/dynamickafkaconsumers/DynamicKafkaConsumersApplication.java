package com.example.dynamickafkaconsumers;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.example.dynamickafkaconsumers.kafka.DynamicKafkaConsumerManager;

@SpringBootApplication
public class DynamicKafkaConsumersApplication {

    public static void main(String[] args) {
        SpringApplication.run(DynamicKafkaConsumersApplication.class, args);
    }

    @Bean
    ApplicationRunner startConsumersOnStartup(DynamicKafkaConsumerManager manager) {
        return args -> manager.startAllFromProperties();
    }
}

