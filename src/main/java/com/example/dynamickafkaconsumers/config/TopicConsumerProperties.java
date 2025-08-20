package com.example.dynamickafkaconsumers.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kafka")
public class TopicConsumerProperties {

    private String bootstrapServers = "localhost:9092";

    private Map<String, TopicConfig> topics = new HashMap<>();

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Map<String, TopicConfig> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, TopicConfig> topics) {
        this.topics = topics;
    }

    public static class TopicConfig {
        private String topicName;
        private String groupId;
        private boolean enableAutoCommit = false;
        private String autoOffsetReset = "latest"; // earliest/latest/none
        private Integer concurrency = 1;
        private Duration pollTimeout = Duration.ofSeconds(3);
        private Map<String, String> properties = new HashMap<>();
        private StartPosition start;
        private String handlerBean;

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public boolean isEnableAutoCommit() {
            return enableAutoCommit;
        }

        public void setEnableAutoCommit(boolean enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
        }

        public String getAutoOffsetReset() {
            return autoOffsetReset;
        }

        public void setAutoOffsetReset(String autoOffsetReset) {
            this.autoOffsetReset = autoOffsetReset;
        }

        public Integer getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(Integer concurrency) {
            this.concurrency = concurrency;
        }

        public Duration getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }

        public StartPosition getStart() {
            return start;
        }

        public void setStart(StartPosition start) {
            this.start = start;
        }

        public String getHandlerBean() {
            return handlerBean;
        }

        public void setHandlerBean(String handlerBean) {
            this.handlerBean = handlerBean;
        }
    }

    public static class StartPosition {
        private Long timestampMs;
        private Map<Integer, Long> partitionToOffset = new HashMap<>();

        public Long getTimestampMs() {
            return timestampMs;
        }

        public void setTimestampMs(Long timestampMs) {
            this.timestampMs = timestampMs;
        }

        public Map<Integer, Long> getPartitionToOffset() {
            return partitionToOffset;
        }

        public void setPartitionToOffset(Map<Integer, Long> partitionToOffset) {
            this.partitionToOffset = partitionToOffset;
        }
    }
}

