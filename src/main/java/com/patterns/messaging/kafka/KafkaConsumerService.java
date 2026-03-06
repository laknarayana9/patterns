package com.patterns.messaging.kafka;

import java.util.List;
import java.util.function.Consumer;

/**
 * Interface for Kafka consumer service with best practices.
 * Designed to be easily mockable for testing.
 */
public interface KafkaConsumerService<T> {
    
    /**
     * Subscribes to topics and starts consuming messages.
     *
     * @param topics the topics to subscribe to
     * @param handler the message handler
     */
    void subscribe(List<String> topics, MessageHandler<T> handler);
    
    /**
     * Subscribes to a single topic and starts consuming messages.
     *
     * @param topic the topic to subscribe to
     * @param handler the message handler
     */
    void subscribe(String topic, MessageHandler<T> handler);
    
    /**
     * Subscribes to topics with a filter.
     *
     * @param topics the topics to subscribe to
     * @param handler the message handler
     * @param filter the message filter
     */
    void subscribe(List<String> topics, MessageHandler<T> handler, MessageFilter<T> filter);
    
    /**
     * Polls for messages once (manual polling mode).
     *
     * @param timeout the timeout in milliseconds
     * @return list of consumed messages
     */
    List<ConsumerRecord<T>> poll(long timeout);
    
    /**
     * Starts the consumer in continuous mode.
     */
    void start();
    
    /**
     * Stops the consumer.
     */
    void stop();
    
    /**
     * Commits the current offsets.
     */
    void commit();
    
    /**
     * Commits offsets asynchronously.
     */
    void commitAsync();
    
    /**
     * Seeks to a specific offset.
     *
     * @param topic the topic
     * @param partition the partition
     * @param offset the offset
     */
    void seek(String topic, int partition, long offset);
    
    /**
     * Pauses consumption.
     */
    void pause();
    
    /**
     * Resumes consumption.
     */
    void resume();
    
    /**
     * Gets consumer metrics.
     *
     * @return consumer metrics
     */
    ConsumerMetrics getMetrics();
    
    /**
     * Closes the consumer and releases resources.
     */
    void close();
    
    /**
     * Functional interface for handling messages.
     *
     * @param <T> the message type
     */
    @FunctionalInterface
    interface MessageHandler<T> {
        void handle(KafkaMessage<T> message) throws Exception;
    }
    
    /**
     * Functional interface for filtering messages.
     *
     * @param <T> the message type
     */
    @FunctionalInterface
    interface MessageFilter<T> {
        boolean accept(KafkaMessage<T> message);
    }
    
    /**
     * Represents a consumed record.
     *
     * @param <T> the message type
     */
    class ConsumerRecord<T> {
        private final String topic;
        private final int partition;
        private final long offset;
        private final String key;
        private final T value;
        private final long timestamp;
        private final String messageId;
        private final String correlationId;
        
        public ConsumerRecord(String topic, int partition, long offset, String key, T value, 
                           long timestamp, String messageId, String correlationId) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.messageId = messageId;
            this.correlationId = correlationId;
        }
        
        public String getTopic() {
            return topic;
        }
        
        public int getPartition() {
            return partition;
        }
        
        public long getOffset() {
            return offset;
        }
        
        public String getKey() {
            return key;
        }
        
        public T getValue() {
            return value;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        public String getMessageId() {
            return messageId;
        }
        
        public String getCorrelationId() {
            return correlationId;
        }
        
        public KafkaMessage<T> toKafkaMessage() {
            return KafkaMessage.builder(topic, value)
                    .key(key)
                    .partition(partition)
                    .timestamp(timestamp)
                    .messageId(messageId)
                    .correlationId(correlationId)
                    .build();
        }
    }
    
    /**
     * Consumer metrics interface.
     */
    interface ConsumerMetrics {
        long getMessagesConsumed();
        long getMessagesFailed();
        double getAverageProcessingTime();
        long getCurrentLag();
        int getActivePartitions();
        boolean isHealthy();
    }
}
