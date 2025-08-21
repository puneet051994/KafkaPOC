package com.example.dynamickafkaconsumers.processing;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageHandler {
    void handle(ConsumerRecord<String, Object> record) throws Exception;
}

