package com.example.dynamickafkaconsumers.processing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("ordersHandler")
public class OrdersMessageHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(OrdersMessageHandler.class);

    @Override
    public void handle(ConsumerRecord<String, Object> record) throws Exception {
        log.info("[Orders] Processing key={} value={} partition={} offset={}", record.key(), record.value(), record.partition(), record.offset());
        // TODO: add domain-specific processing for orders
    }
}

