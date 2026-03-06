package com.patterns.messaging.kafka;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration utilities for Kafka with best practices.
 * Provides builder patterns for producer and consumer configurations.
 */
public class KafkaConfig {
    
    /**
     * Default configuration values following Kafka best practices.
     */
    public static class Defaults {
        public static final String BOOTSTRAP_SERVERS = "localhost:9092";
        public static final String CLIENT_ID_PREFIX = "enterprise-patterns";
        public static final int DEFAULT_PARTITIONS = 3;
        public static final short REPLICATION_FACTOR = 1;
        
        // Producer defaults
        public static final int PRODUCER_BATCH_SIZE = 16384;
        public static final int PRODUCER_LINGER_MS = 5;
        public static final int PRODUCER_RETRIES = 3;
        public static final long PRODUCER_REQUEST_TIMEOUT_MS = 30000;
        public static final long PRODUCER_DELIVERY_TIMEOUT_MS = 120000;
        public static final String PRODUCER_COMPRESSION_TYPE = "lz4";
        public static final String PRODUCER_ACKS = "all";
        
        // Consumer defaults
        public static final String CONSUMER_AUTO_OFFSET_RESET = "latest";
        public static final boolean CONSUMER_ENABLE_AUTO_COMMIT = false;
        public static final int CONSUMER_MAX_POLL_RECORDS = 500;
        public static final Duration CONSUMER_POLL_TIMEOUT = Duration.ofMillis(1000);
        public static final Duration CONSUMER_SESSION_TIMEOUT = Duration.ofSeconds(30);
        public static final Duration CONSUMER_HEARTBEAT_INTERVAL = Duration.ofSeconds(3);
        public static final Duration CONSUMER_MAX_POLL_INTERVAL = Duration.ofMinutes(5);
    }
    
    /**
     * Builder for Kafka producer configuration.
     */
    public static class ProducerConfigBuilder {
        private final Properties properties = new Properties();
        
        public ProducerConfigBuilder() {
            // Set sensible defaults
            bootstrapServers(Defaults.BOOTSTRAP_SERVERS);
            clientId(Defaults.CLIENT_ID_PREFIX + "-producer");
            batchSize(Defaults.PRODUCER_BATCH_SIZE);
            lingerMs(Defaults.PRODUCER_LINGER_MS);
            retries(Defaults.PRODUCER_RETRIES);
            requestTimeoutMs(Defaults.PRODUCER_REQUEST_TIMEOUT_MS);
            deliveryTimeoutMs(Defaults.PRODUCER_DELIVERY_TIMEOUT_MS);
            compressionType(Defaults.PRODUCER_COMPRESSION_TYPE);
            acks(Defaults.PRODUCER_ACKS);
            
            // Key and value serializers (assuming String for simplicity)
            keySerializer("org.apache.kafka.common.serialization.StringSerializer");
            valueSerializer("org.apache.kafka.common.serialization.StringSerializer");
            
            // Enable idempotence for exactly-once semantics
            enableIdempotence(true);
        }
        
        public ProducerConfigBuilder bootstrapServers(String servers) {
            properties.put("bootstrap.servers", servers);
            return this;
        }
        
        public ProducerConfigBuilder clientId(String clientId) {
            properties.put("client.id", clientId);
            return this;
        }
        
        public ProducerConfigBuilder keySerializer(String serializer) {
            properties.put("key.serializer", serializer);
            return this;
        }
        
        public ProducerConfigBuilder valueSerializer(String serializer) {
            properties.put("value.serializer", serializer);
            return this;
        }
        
        public ProducerConfigBuilder batchSize(int size) {
            properties.put("batch.size", size);
            return this;
        }
        
        public ProducerConfigBuilder lingerMs(int ms) {
            properties.put("linger.ms", ms);
            return this;
        }
        
        public ProducerConfigBuilder retries(int retries) {
            properties.put("retries", retries);
            return this;
        }
        
        public ProducerConfigBuilder requestTimeoutMs(long ms) {
            properties.put("request.timeout.ms", ms);
            return this;
        }
        
        public ProducerConfigBuilder deliveryTimeoutMs(long ms) {
            properties.put("delivery.timeout.ms", ms);
            return this;
        }
        
        public ProducerConfigBuilder compressionType(String type) {
            properties.put("compression.type", type);
            return this;
        }
        
        public ProducerConfigBuilder acks(String acks) {
            properties.put("acks", acks);
            return this;
        }
        
        public ProducerConfigBuilder enableIdempotence(boolean enable) {
            properties.put("enable.idempotence", enable);
            return this;
        }
        
        public ProducerConfigBuilder maxInFlightRequestsPerConnection(int max) {
            properties.put("max.in.flight.requests.per.connection", max);
            return this;
        }
        
        public ProducerConfigBuilder bufferMemory(long bytes) {
            properties.put("buffer.memory", bytes);
            return this;
        }
        
        public ProducerConfigBuilder metricReporterClasses(String reporters) {
            properties.put("metric.reporters", reporters);
            return this;
        }
        
        public ProducerConfigBuilder customProperty(String key, Object value) {
            properties.put(key, value);
            return this;
        }
        
        public Properties build() {
            return new Properties(properties);
        }
        
        public Map<String, Object> buildMap() {
            Map<String, Object> map = new HashMap<>();
            properties.forEach((key, value) -> map.put(key.toString(), value));
            return map;
        }
    }
    
    /**
     * Builder for Kafka consumer configuration.
     */
    public static class ConsumerConfigBuilder {
        private final Properties properties = new Properties();
        
        public ConsumerConfigBuilder() {
            // Set sensible defaults
            bootstrapServers(Defaults.BOOTSTRAP_SERVERS);
            clientId(Defaults.CLIENT_ID_PREFIX + "-consumer");
            groupId(Defaults.CLIENT_ID_PREFIX + "-group");
            autoOffsetReset(Defaults.CONSUMER_AUTO_OFFSET_RESET);
            enableAutoCommit(Defaults.CONSUMER_ENABLE_AUTO_COMMIT);
            maxPollRecords(Defaults.CONSUMER_MAX_POLL_RECORDS);
            sessionTimeoutMs((int) Defaults.CONSUMER_SESSION_TIMEOUT.toMillis());
            heartbeatIntervalMs((int) Defaults.CONSUMER_HEARTBEAT_INTERVAL.toMillis());
            maxPollIntervalMs((int) Defaults.CONSUMER_MAX_POLL_INTERVAL.toMillis());
            
            // Key and value deserializers (assuming String for simplicity)
            keyDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
            valueDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
        }
        
        public ConsumerConfigBuilder bootstrapServers(String servers) {
            properties.put("bootstrap.servers", servers);
            return this;
        }
        
        public ConsumerConfigBuilder clientId(String clientId) {
            properties.put("client.id", clientId);
            return this;
        }
        
        public ConsumerConfigBuilder groupId(String groupId) {
            properties.put("group.id", groupId);
            return this;
        }
        
        public ConsumerConfigBuilder keyDeserializer(String deserializer) {
            properties.put("key.deserializer", deserializer);
            return this;
        }
        
        public ConsumerConfigBuilder valueDeserializer(String deserializer) {
            properties.put("value.deserializer", deserializer);
            return this;
        }
        
        public ConsumerConfigBuilder autoOffsetReset(String reset) {
            properties.put("auto.offset.reset", reset);
            return this;
        }
        
        public ConsumerConfigBuilder enableAutoCommit(boolean enable) {
            properties.put("enable.auto.commit", enable);
            return this;
        }
        
        public ConsumerConfigBuilder autoCommitIntervalMs(int ms) {
            properties.put("auto.commit.interval.ms", ms);
            return this;
        }
        
        public ConsumerConfigBuilder maxPollRecords(int max) {
            properties.put("max.poll.records", max);
            return this;
        }
        
        public ConsumerConfigBuilder sessionTimeoutMs(int ms) {
            properties.put("session.timeout.ms", ms);
            return this;
        }
        
        public ConsumerConfigBuilder heartbeatIntervalMs(int ms) {
            properties.put("heartbeat.interval.ms", ms);
            return this;
        }
        
        public ConsumerConfigBuilder maxPollIntervalMs(int ms) {
            properties.put("max.poll.interval.ms", ms);
            return this;
        }
        
        public ConsumerConfigBuilder fetchMinBytes(int bytes) {
            properties.put("fetch.min.bytes", bytes);
            return this;
        }
        
        public ConsumerConfigBuilder fetchMaxWaitMs(int ms) {
            properties.put("fetch.max.wait.ms", ms);
            return this;
        }
        
        public ConsumerConfigBuilder maxPartitionFetchBytes(int bytes) {
            properties.put("max.partition.fetch.bytes", bytes);
            return this;
        }
        
        public ConsumerConfigBuilder metricReporterClasses(String reporters) {
            properties.put("metric.reporters", reporters);
            return this;
        }
        
        public ConsumerConfigBuilder customProperty(String key, Object value) {
            properties.put(key, value);
            return this;
        }
        
        public Properties build() {
            return new Properties(properties);
        }
        
        public Map<String, Object> buildMap() {
            Map<String, Object> map = new HashMap<>();
            properties.forEach((key, value) -> map.put(key.toString(), value));
            return map;
        }
    }
    
    /**
     * Environment-specific configurations.
     */
    public static class Environment {
        
        public static ProducerConfigBuilder producerForDev() {
            return new ProducerConfigBuilder()
                    .bootstrapServers("localhost:9092")
                    .acks("1") // Faster but less durable
                    .retries(1)
                    .clientId("dev-producer");
        }
        
        public static ProducerConfigBuilder producerForStaging() {
            return new ProducerConfigBuilder()
                    .bootstrapServers("staging-kafka:9092")
                    .acks("all")
                    .retries(3)
                    .clientId("staging-producer");
        }
        
        public static ProducerConfigBuilder producerForProduction() {
            return new ProducerConfigBuilder()
                    .bootstrapServers("prod-kafka-1:9092,prod-kafka-2:9092,prod-kafka-3:9092")
                    .acks("all")
                    .retries(5)
                    .clientId("prod-producer")
                    .enableIdempotence(true)
                    .compressionType("snappy");
        }
        
        public static ConsumerConfigBuilder consumerForDev() {
            return new ConsumerConfigBuilder()
                    .bootstrapServers("localhost:9092")
                    .groupId("dev-consumer-group")
                    .clientId("dev-consumer");
        }
        
        public static ConsumerConfigBuilder consumerForStaging() {
            return new ConsumerConfigBuilder()
                    .bootstrapServers("staging-kafka:9092")
                    .groupId("staging-consumer-group")
                    .clientId("staging-consumer");
        }
        
        public static ConsumerConfigBuilder consumerForProduction() {
            return new ConsumerConfigBuilder()
                    .bootstrapServers("prod-kafka-1:9092,prod-kafka-2:9092,prod-kafka-3:9092")
                    .groupId("prod-consumer-group")
                    .clientId("prod-consumer")
                    .enableAutoCommit(false)
                    .maxPollRecords(1000);
        }
    }
    
    /**
     * Utility methods for common configurations.
     */
    public static class Utils {
        
        public static Properties createDefaultProducerConfig() {
            return new ProducerConfigBuilder().build();
        }
        
        public static Properties createDefaultConsumerConfig() {
            return new ConsumerConfigBuilder().build();
        }
        
        public static Properties createProducerConfig(String bootstrapServers) {
            return new ProducerConfigBuilder()
                    .bootstrapServers(bootstrapServers)
                    .build();
        }
        
        public static Properties createConsumerConfig(String bootstrapServers, String groupId) {
            return new ConsumerConfigBuilder()
                    .bootstrapServers(bootstrapServers)
                    .groupId(groupId)
                    .build();
        }
        
        public static Map<String, Object> createProducerMap(String bootstrapServers) {
            return new ProducerConfigBuilder()
                    .bootstrapServers(bootstrapServers)
                    .buildMap();
        }
        
        public static Map<String, Object> createConsumerMap(String bootstrapServers, String groupId) {
            return new ConsumerConfigBuilder()
                    .bootstrapServers(bootstrapServers)
                    .groupId(groupId)
                    .buildMap();
        }
    }
}
