package com.patterns.messaging.kafka;

import java.util.Properties;

/**
 * Simple Kafka configuration for interview preparation.
 */
public class KafkaConfig {
    
    // Production-ready defaults
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String PRODUCER_ACKS = "all";
    public static final boolean PRODUCER_IDEMPOTENCE = true;
    public static final short REPLICATION_FACTOR = 3;
    
    // Exactly-once: enable.idempotence=true + acks=all + replication=3
    public static Properties createProducerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", PRODUCER_ACKS);
        props.put("enable.idempotence", PRODUCER_IDEMPOTENCE);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
    
    public static Properties createConsumerConfig(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "latest");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
