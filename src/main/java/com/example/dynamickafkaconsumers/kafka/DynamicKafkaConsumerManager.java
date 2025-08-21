package com.example.dynamickafkaconsumers.kafka;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.example.dynamickafkaconsumers.config.TopicConsumerProperties;
import com.example.dynamickafkaconsumers.config.TopicConsumerProperties.TopicConfig;
import com.example.dynamickafkaconsumers.config.TopicConsumerProperties.StartPosition;
import com.example.dynamickafkaconsumers.processing.MessageHandler;
import com.example.dynamickafkaconsumers.config.SecurityProperties;
import com.example.dynamickafkaconsumers.config.SchemaRegistryProperties;

@Service
public class DynamicKafkaConsumerManager {

    private static final Logger log = LoggerFactory.getLogger(DynamicKafkaConsumerManager.class);

    private final TopicConsumerProperties topicConsumerProperties;
    private final BeanFactory beanFactory;
    private final SecurityProperties securityProperties;
    private final SchemaRegistryProperties schemaRegistryProperties;
    private final Map<String, ConcurrentMessageListenerContainer<String, Object>> topicToContainer = new ConcurrentHashMap<>();

    public DynamicKafkaConsumerManager(TopicConsumerProperties topicConsumerProperties, BeanFactory beanFactory, SecurityProperties securityProperties, SchemaRegistryProperties schemaRegistryProperties) {
        this.topicConsumerProperties = topicConsumerProperties;
        this.beanFactory = beanFactory;
        this.securityProperties = securityProperties;
        this.schemaRegistryProperties = schemaRegistryProperties;
    }

    public synchronized void startConsumer(String topicKey) {
        startConsumer(topicKey, null, null);
    }

    public synchronized void startConsumer(String topicKey, Long startTimestampMs, Map<Integer, Long> partitionToOffset) {
        if (topicToContainer.containsKey(topicKey)) {
            log.info("Consumer for {} already running", topicKey);
            return;
        }

        TopicConfig config = topicConsumerProperties.getTopics().get(topicKey);
        if (config == null) {
            throw new IllegalArgumentException("Unknown topic key: " + topicKey);
        }

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, topicConsumerProperties.getBootstrapServers());
        Object keyDeser = config.getKeyDeserializerClass() != null ? config.getKeyDeserializerClass() : StringDeserializer.class;
        Object valDeser = config.getValueDeserializerClass() != null ? config.getValueDeserializerClass() : StringDeserializer.class;
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeser);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valDeser);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, Objects.requireNonNull(config.getGroupId(), "groupId required"));
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(config.isEnableAutoCommit()));
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Optional.ofNullable(config.getAutoOffsetReset()).orElse("latest"));
        consumerProps.putAll(securityProperties.asKafkaProperties());
        consumerProps.putAll(schemaRegistryProperties.asKafkaProperties());
        if (topicConsumerProperties.getCommonProperties() != null) consumerProps.putAll(topicConsumerProperties.getCommonProperties());
        if (config.getProperties() != null) consumerProps.putAll(config.getProperties());

        DefaultKafkaConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(Optional.ofNullable(config.getConcurrency()).orElse(1));
        factory.getContainerProperties().setAckMode(config.isEnableAutoCommit() ? ContainerProperties.AckMode.BATCH : ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(config.getPollTimeout().toMillis());

        ContainerProperties containerProps = new ContainerProperties(config.getTopicName());
        containerProps.setGroupId(config.getGroupId());
        containerProps.setAckMode(config.isEnableAutoCommit() ? ContainerProperties.AckMode.BATCH : ContainerProperties.AckMode.MANUAL);

        MessageHandler handler = resolveHandler(config.getHandlerBean());
        containerProps.setMessageListener((AcknowledgingConsumerAwareMessageListener<String, Object>) (record, acknowledgment, consumer) -> {
            try {
                if (handler != null) {
                    handler.handle((ConsumerRecord<String, Object>) record);
                } else {
                    log.info("[{}] Consumed record topic={} partition={} offset={} key={} value={}", topicKey, record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                if (!config.isEnableAutoCommit()) {
                    acknowledgment.acknowledge();
                }
            } catch (Exception e) {
                log.error("Error processing record", e);
            }
        });

        ConcurrentMessageListenerContainer<String, Object> container = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        if (config.getConcurrency() != null && config.getConcurrency() > 1) {
            container.setConcurrency(config.getConcurrency());
        }
        if (config.getPollTimeout() != null) {
            container.getContainerProperties().setPollTimeout(config.getPollTimeout().toMillis());
        }

        if (startTimestampMs != null || (partitionToOffset != null && !partitionToOffset.isEmpty())) {
            container.getContainerProperties().setConsumerRebalanceListener(new SeekingRebalanceListener(config.getTopicName(), startTimestampMs, partitionToOffset, topicConsumerProperties.getBootstrapServers()));
        }

        container.start();
        topicToContainer.put(topicKey, container);
        log.info("Started consumer for {} -> topic {}", topicKey, config.getTopicName());
    }

    private MessageHandler resolveHandler(String handlerBeanName) {
        if (handlerBeanName == null || handlerBeanName.isBlank()) {
            return null;
        }
        try {
            return beanFactory.getBean(handlerBeanName, MessageHandler.class);
        } catch (Exception e) {
            log.warn("Handler bean '{}' not found; falling back to default logging handler", handlerBeanName);
            return null;
        }
    }

    public synchronized void startAllFromProperties() {
        topicConsumerProperties.getTopics().forEach((topicKey, config) -> {
            StartPosition start = config.getStart();
            Long ts = start == null ? null : start.getTimestampMs();
            Map<Integer, Long> offsets = start == null ? null : start.getPartitionToOffset();
            startConsumer(topicKey, ts, offsets);
        });
    }

    public synchronized void stopConsumer(String topicKey) {
        ConcurrentMessageListenerContainer<String, Object> container = topicToContainer.remove(topicKey);
        if (container != null) {
            container.stop();
            log.info("Stopped consumer for {}", topicKey);
        }
    }

    public synchronized void restartConsumer(String topicKey) {
        stopConsumer(topicKey);
        startConsumer(topicKey);
    }

    public synchronized Collection<String> listRunning() {
        return List.copyOf(topicToContainer.keySet());
    }

    static class SeekingRebalanceListener implements org.springframework.kafka.listener.ConsumerAwareRebalanceListener {
        private final String topic;
        private final Long startTimestampMs;
        private final Map<Integer, Long> partitionToOffset;
        private final String bootstrapServers;

        SeekingRebalanceListener(String topic, Long startTimestampMs, Map<Integer, Long> partitionToOffset, String bootstrapServers) {
            this.topic = topic;
            this.startTimestampMs = startTimestampMs;
            this.partitionToOffset = partitionToOffset == null ? Map.of() : partitionToOffset;
            this.bootstrapServers = bootstrapServers;
        }

        @Override
        public void onPartitionsAssigned(org.apache.kafka.clients.consumer.Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) {
                return;
            }
            Map<TopicPartition, Long> seekTimestamps = new HashMap<>();
            Map<TopicPartition, Long> seekOffsets = new HashMap<>();

            for (TopicPartition tp : partitions) {
                if (!tp.topic().equals(topic)) {
                    continue;
                }
                if (partitionToOffset.containsKey(tp.partition())) {
                    seekOffsets.put(tp, partitionToOffset.get(tp.partition()));
                } else if (startTimestampMs != null) {
                    seekTimestamps.put(tp, startTimestampMs);
                }
            }

            if (!seekOffsets.isEmpty()) {
                seekOffsets.forEach((tp, offset) -> {
                    ((org.apache.kafka.clients.consumer.Consumer<?, ?>) consumer).seek(tp, offset);
                });
            }

            if (!seekTimestamps.isEmpty()) {
                try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
                    Map<TopicPartition, Instant> query = new HashMap<>();
                    seekTimestamps.forEach((tp, ts) -> query.put(tp, Instant.ofEpochMilli(ts)));
                    ListOffsetsResult result = admin.listOffsets(convertToOffsetsSpec(query));
                    result.all().get().forEach((tp, offSpec) -> {
                        long offset = offSpec.offset();
                        ((org.apache.kafka.clients.consumer.Consumer<?, ?>) consumer).seek(tp, offset);
                    });
                } catch (Exception e) {
                    // Fallback to consumer offsetsForTimes when available
                    Map<TopicPartition, Long> tsMap = new HashMap<>();
                    seekTimestamps.forEach((tp, ts) -> tsMap.put(tp, ts));
                    Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsetsForTimes = ((org.apache.kafka.clients.consumer.Consumer<?, ?>) consumer).offsetsForTimes(tsMap);
                    offsetsForTimes.forEach((tp, oat) -> {
                        if (oat != null) {
                            ((org.apache.kafka.clients.consumer.Consumer<?, ?>) consumer).seek(tp, oat.offset());
                        }
                    });
                }
            }
        }

        private Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> convertToOffsetsSpec(Map<TopicPartition, Instant> query) {
            Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> spec = new HashMap<>();
            query.forEach((tp, instant) -> spec.put(tp, org.apache.kafka.clients.admin.OffsetSpec.forTimestamp(instant.toEpochMilli())));
            return spec;
        }
    }
}

