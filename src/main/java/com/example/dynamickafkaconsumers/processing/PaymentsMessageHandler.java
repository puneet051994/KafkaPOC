package com.example.dynamickafkaconsumers.processing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("paymentsHandler")
public class PaymentsMessageHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(PaymentsMessageHandler.class);

    @Override
    public void handle(ConsumerRecord<String, String> record) throws Exception {
        log.info("[Payments] Processing key={} value={} partition={} offset={}", record.key(), record.value(), record.partition(), record.offset());
        // TODO: add domain-specific processing for payments
    }
}

