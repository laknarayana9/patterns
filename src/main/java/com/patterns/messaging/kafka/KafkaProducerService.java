package com.patterns.messaging.kafka;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Interface for Kafka producer service with best practices.
 * Designed to be easily mockable for testing.
 */
public interface KafkaProducerService<T> {
    
    /**
     * Sends a message to Kafka asynchronously.
     *
     * @param message the message to send
     * @return a CompletableFuture that completes when the message is sent
     */
    CompletableFuture<SendResult> sendAsync(KafkaMessage<T> message);
    
    /**
     * Sends a message to Kafka synchronously.
     *
     * @param message the message to send
     * @return the send result
     * @throws KafkaSendException if the send fails
     */
    SendResult sendSync(KafkaMessage<T> message) throws KafkaSendException;
    
    /**
     * Sends a message to a specific topic with key and value.
     *
     * @param topic the topic to send to
     * @param key the message key (can be null)
     * @param value the message value
     * @return a CompletableFuture that completes when the message is sent
     */
    CompletableFuture<SendResult> sendAsync(String topic, String key, T value);
    
    /**
     * Sends a message to a specific topic with value only.
     *
     * @param topic the topic to send to
     * @param value the message value
     * @return a CompletableFuture that completes when the message is sent
     */
    CompletableFuture<SendResult> sendAsync(String topic, T value);
    
    /**
     * Flushes any pending messages.
     */
    void flush();
    
    /**
     * Closes the producer and releases resources.
     */
    void close();
    
    /**
     * Gets producer metrics.
     *
     * @return producer metrics
     */
    ProducerMetrics getMetrics();
    
    /**
     * Result of a send operation.
     */
    class SendResult {
        private final String topic;
        private final Integer partition;
        private final Long offset;
        private final String messageId;
        private final long timestamp;
        
        public SendResult(String topic, Integer partition, Long offset, String messageId, long timestamp) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.messageId = messageId;
            this.timestamp = timestamp;
        }
        
        public String getTopic() {
            return topic;
        }
        
        public Integer getPartition() {
            return partition;
        }
        
        public Long getOffset() {
            return offset;
        }
        
        public String getMessageId() {
            return messageId;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("SendResult{topic='%s', partition=%d, offset=%d, messageId='%s'}",
                    topic, partition, offset, messageId);
        }
    }
    
    /**
     * Exception thrown when a send operation fails.
     */
    class KafkaSendException extends Exception {
        public KafkaSendException(String message) {
            super(message);
        }
        
        public KafkaSendException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
    /**
     * Producer metrics interface.
     */
    interface ProducerMetrics {
        long getMessagesSent();
        long getMessagesFailed();
        double getAverageSendTime();
        long getBytesSent();
        int getBufferSize();
        boolean isHealthy();
    }
}
