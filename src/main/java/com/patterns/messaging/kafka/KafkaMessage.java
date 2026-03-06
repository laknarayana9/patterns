package com.patterns.messaging.kafka;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Kafka-specific message wrapper with headers and metadata.
 * Follows Kafka best practices for message structure.
 */
public class KafkaMessage<T> {
    private final String key;
    private final T value;
    private final String topic;
    private final Integer partition;
    private final Long timestamp;
    private final Map<String, String> headers;
    private final String messageId;
    private final String correlationId;
    private final String sourceSystem;
    
    private KafkaMessage(Builder<T> builder) {
        this.key = builder.key;
        this.value = Objects.requireNonNull(builder.value, "Message value cannot be null");
        this.topic = Objects.requireNonNull(builder.topic, "Topic cannot be null");
        this.partition = builder.partition;
        this.timestamp = builder.timestamp != null ? builder.timestamp : System.currentTimeMillis();
        this.headers = new HashMap<>(builder.headers);
        this.messageId = builder.messageId != null ? builder.messageId : UUID.randomUUID().toString();
        this.correlationId = builder.correlationId;
        this.sourceSystem = builder.sourceSystem;
    }
    
    public String getKey() {
        return key;
    }
    
    public T getValue() {
        return value;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public Integer getPartition() {
        return partition;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public Map<String, String> getHeaders() {
        return new HashMap<>(headers);
    }
    
    public String getMessageId() {
        return messageId;
    }
    
    public String getCorrelationId() {
        return correlationId;
    }
    
    public String getSourceSystem() {
        return sourceSystem;
    }
    
    public boolean hasCorrelationId() {
        return correlationId != null;
    }
    
    public Builder<T> toBuilder() {
        return new Builder<T>()
                .key(key)
                .value(value)
                .topic(topic)
                .partition(partition)
                .timestamp(timestamp)
                .headers(headers)
                .messageId(messageId)
                .correlationId(correlationId)
                .sourceSystem(sourceSystem);
    }
    
    public static <T> Builder<T> builder(String topic, T value) {
        return new Builder<T>().topic(topic).value(value);
    }
    
    public static <T> KafkaMessage<T> of(String topic, T value) {
        return new Builder<T>().topic(topic).value(value).build();
    }
    
    public static <T> KafkaMessage<T> of(String key, String topic, T value) {
        return new Builder<T>().key(key).topic(topic).value(value).build();
    }
    
    @Override
    public String toString() {
        return String.format("KafkaMessage{messageId='%s', topic='%s', key='%s', timestamp=%d, correlationId='%s'}",
                messageId, topic, key, timestamp, correlationId);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaMessage<?> that = (KafkaMessage<?>) o;
        return Objects.equals(messageId, that.messageId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(messageId);
    }
    
    public static class Builder<T> {
        private String key;
        private T value;
        private String topic;
        private Integer partition;
        private Long timestamp;
        private Map<String, String> headers = new HashMap<>();
        private String messageId;
        private String correlationId;
        private String sourceSystem;
        
        public Builder<T> key(String key) {
            this.key = key;
            return this;
        }
        
        public Builder<T> value(T value) {
            this.value = value;
            return this;
        }
        
        public Builder<T> topic(String topic) {
            this.topic = topic;
            return this;
        }
        
        public Builder<T> partition(Integer partition) {
            this.partition = partition;
            return this;
        }
        
        public Builder<T> timestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        
        public Builder<T> headers(Map<String, String> headers) {
            this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
            return this;
        }
        
        public Builder<T> header(String key, String value) {
            this.headers.put(key, value);
            return this;
        }
        
        public Builder<T> messageId(String messageId) {
            this.messageId = messageId;
            return this;
        }
        
        public Builder<T> correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }
        
        public Builder<T> sourceSystem(String sourceSystem) {
            this.sourceSystem = sourceSystem;
            return this;
        }
        
        public Builder<T> withStandardHeaders() {
            this.header("message-id", messageId != null ? messageId : UUID.randomUUID().toString());
            this.header("timestamp", String.valueOf(timestamp != null ? timestamp : System.currentTimeMillis()));
            if (correlationId != null) {
                this.header("correlation-id", correlationId);
            }
            if (sourceSystem != null) {
                this.header("source-system", sourceSystem);
            }
            return this;
        }
        
        public KafkaMessage<T> build() {
            return new KafkaMessage<>(this);
        }
    }
}
