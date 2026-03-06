package com.patterns.messaging.kafka;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * High-level Kafka template abstraction that combines producer and consumer operations.
 * Provides a clean API for common Kafka operations and is easily mockable.
 */
public class KafkaTemplate<T> {
    
    private final KafkaProducerService<T> producer;
    private final KafkaConsumerService<T> consumer;
    
    public KafkaTemplate(KafkaProducerService<T> producer, KafkaConsumerService<T> consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }
    
    /**
     * Sends a message asynchronously.
     */
    public CompletableFuture<KafkaProducerService.SendResult> send(KafkaMessage<T> message) {
        return producer.sendAsync(message);
    }
    
    /**
     * Sends a message with topic and value.
     */
    public CompletableFuture<KafkaProducerService.SendResult> send(String topic, T value) {
        return producer.sendAsync(topic, value);
    }
    
    /**
     * Sends a message with topic, key, and value.
     */
    public CompletableFuture<KafkaProducerService.SendResult> send(String topic, String key, T value) {
        return producer.sendAsync(topic, key, value);
    }
    
    /**
     * Sends a message synchronously.
     */
    public KafkaProducerService.SendResult sendSync(KafkaMessage<T> message) throws KafkaProducerService.KafkaSendException {
        return producer.sendSync(message);
    }
    
    /**
     * Subscribes to topics with a handler.
     */
    public void subscribe(List<String> topics, KafkaConsumerService.MessageHandler<T> handler) {
        consumer.subscribe(topics, handler);
    }
    
    /**
     * Subscribes to a single topic with a handler.
     */
    public void subscribe(String topic, KafkaConsumerService.MessageHandler<T> handler) {
        consumer.subscribe(topic, handler);
    }
    
    /**
     * Subscribes to topics with handler and filter.
     */
    public void subscribe(List<String> topics, KafkaConsumerService.MessageHandler<T> handler, 
                         KafkaConsumerService.MessageFilter<T> filter) {
        consumer.subscribe(topics, handler, filter);
    }
    
    /**
     * Sends a message and waits for a response (request-reply pattern).
     */
    public CompletableFuture<T> sendAndReceive(String requestTopic, String replyTopic, 
                                              T request, String correlationId, long timeout) {
        String messageId = java.util.UUID.randomUUID().toString();
        KafkaMessage<T> message = KafkaMessage.builder(requestTopic, request)
                .correlationId(correlationId)
                .messageId(messageId)
                .build();
        
        CompletableFuture<T> responseFuture = new CompletableFuture<>();
        
        // Subscribe to reply topic with correlation filter
        subscribe(List.of(replyTopic), 
            (replyMessage) -> {
                if (correlationId.equals(replyMessage.getCorrelationId())) {
                    responseFuture.complete(replyMessage.getValue());
                }
            },
            (replyMessage) -> correlationId.equals(replyMessage.getCorrelationId())
        );
        
        // Send the request
        send(message).exceptionally(throwable -> {
            responseFuture.completeExceptionally(throwable);
            return null;
        });
        
        // Add timeout
        java.util.concurrent.CompletableFuture.delayedExecutor(timeout, java.util.concurrent.TimeUnit.MILLISECONDS)
            .execute(() -> {
                if (!responseFuture.isDone()) {
                    responseFuture.completeExceptionally(
                        new java.util.concurrent.TimeoutException("Response timeout"));
                }
            });
        
        return responseFuture;
    }
    
    /**
     * Starts the consumer.
     */
    public void startConsumer() {
        consumer.start();
    }
    
    /**
     * Stops the consumer.
     */
    public void stopConsumer() {
        consumer.stop();
    }
    
    /**
     * Flushes pending messages.
     */
    public void flush() {
        producer.flush();
    }
    
    /**
     * Commits consumer offsets.
     */
    public void commit() {
        consumer.commit();
    }
    
    /**
     * Commits consumer offsets asynchronously.
     */
    public void commitAsync() {
        consumer.commitAsync();
    }
    
    /**
     * Gets combined metrics.
     */
    public KafkaMetrics getMetrics() {
        return new KafkaMetrics(producer.getMetrics(), consumer.getMetrics());
    }
    
    /**
     * Closes both producer and consumer.
     */
    public void close() {
        producer.close();
        consumer.close();
    }
    
    /**
     * Combined metrics for producer and consumer.
     */
    public static class KafkaMetrics {
        private final KafkaProducerService.ProducerMetrics producerMetrics;
        private final KafkaConsumerService.ConsumerMetrics consumerMetrics;
        
        public KafkaMetrics(KafkaProducerService.ProducerMetrics producerMetrics, 
                          KafkaConsumerService.ConsumerMetrics consumerMetrics) {
            this.producerMetrics = producerMetrics;
            this.consumerMetrics = consumerMetrics;
        }
        
        public KafkaProducerService.ProducerMetrics getProducerMetrics() {
            return producerMetrics;
        }
        
        public KafkaConsumerService.ConsumerMetrics getConsumerMetrics() {
            return consumerMetrics;
        }
        
        public boolean isHealthy() {
            return producerMetrics.isHealthy() && consumerMetrics.isHealthy();
        }
        
        @Override
        public String toString() {
            return String.format("KafkaMetrics{producer=%s, consumer=%s, healthy=%s}",
                    producerMetrics, consumerMetrics, isHealthy());
        }
    }
}
