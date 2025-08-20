package com.example.dynamickafkaconsumers.processing;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageHandler {
    void handle(ConsumerRecord<String, String> record) throws Exception;
}

