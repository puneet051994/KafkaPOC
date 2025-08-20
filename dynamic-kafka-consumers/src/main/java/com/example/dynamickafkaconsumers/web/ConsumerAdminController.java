package com.example.dynamickafkaconsumers.web;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.dynamickafkaconsumers.kafka.DynamicKafkaConsumerManager;

@RestController
@RequestMapping("/api/consumers")
public class ConsumerAdminController {

    private final DynamicKafkaConsumerManager manager;

    public ConsumerAdminController(DynamicKafkaConsumerManager manager) {
        this.manager = manager;
    }

    @GetMapping
    public ResponseEntity<?> list() {
        return ResponseEntity.ok(manager.listRunning());
    }

    @PostMapping("/{topicKey}/start")
    public ResponseEntity<?> start(@PathVariable String topicKey, @RequestBody(required = false) StartRequest request) {
        Long ts = request == null ? null : request.timestampMs;
        Map<Integer, Long> offsets = request == null ? null : request.partitionToOffset;
        manager.startConsumer(topicKey, ts, offsets);
        return ResponseEntity.accepted().build();
    }

    @DeleteMapping("/{topicKey}")
    public ResponseEntity<?> stop(@PathVariable String topicKey) {
        manager.stopConsumer(topicKey);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{topicKey}/restart")
    public ResponseEntity<?> restart(@PathVariable String topicKey) {
        manager.restartConsumer(topicKey);
        return ResponseEntity.accepted().build();
    }

    public static class StartRequest {
        public Long timestampMs; // optional
        public Map<Integer, Long> partitionToOffset; // optional
    }
}

