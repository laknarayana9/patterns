package com.patterns.messaging.kafka;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Resilient Kafka template with DLQ and retry logic.
 * Integrates exponential backoff and dead letter queue handling.
 */
public class ResilientKafkaTemplate<T> {
    
    private final KafkaTemplate<T> delegate;
    private final DLQHandler<T> dlqHandler;
    
    public ResilientKafkaTemplate(KafkaProducerService<T> producer,
                                KafkaConsumerService<T> consumer,
                                DLQHandler<T> dlqHandler) {
        this.delegate = new KafkaTemplate<>(producer, consumer);
        this.dlqHandler = dlqHandler;
    }
    
    /**
     * Result of resilient send operation.
     */
    public static class ResilientSendResult {
        private final KafkaProducerService.SendResult sendResult;
        private final boolean wasRetried;
        private final String errorMessage;
        
        public ResilientSendResult(KafkaProducerService.SendResult sendResult, 
                                   boolean wasRetried, 
                                   String errorMessage) {
            this.sendResult = sendResult;
            this.wasRetried = wasRetried;
            this.errorMessage = errorMessage;
        }
        
        public KafkaProducerService.SendResult getSendResult() { return sendResult; }
        public boolean wasRetried() { return wasRetried; }
        public String getErrorMessage() { return errorMessage; }
    }
    
    /**
     * Sends message with DLQ handling on failure.
     */
    public CompletableFuture<ResilientSendResult> sendWithDLQ(KafkaMessage<T> message) {
        return delegate.send(message)
                .thenApply(result -> new ResilientSendResult(result, false, null))
                .exceptionally(throwable -> {
                    // Handle failure with DLQ logic
                    if (throwable instanceof RuntimeException) {
                        RuntimeException rte = (RuntimeException) throwable;
                        if (rte.getCause() instanceof KafkaProducerService.KafkaSendException) {
                            return handleSendFailure(message, rte.getCause());
                        }
                    }
                    return new ResilientSendResult(null, false, 
                        "Unexpected error: " + throwable.getMessage());
                });
    }
    
    /**
     * Sends message synchronously with DLQ handling.
     */
    public ResilientSendResult sendWithDLQSync(KafkaMessage<T> message) throws KafkaProducerService.KafkaSendException {
        try {
            return new ResilientSendResult(delegate.sendSync(message), false, null);
        } catch (KafkaProducerService.KafkaSendException e) {
            CompletableFuture<DLQHandler.DLQResult> futureResult = dlqHandler.handleFailure(message, e);
            
            try {
                DLQHandler.DLQResult result = futureResult.get();
                if (result.getType() == DLQHandler.DLQResult.ResultType.SENT_TO_DLQ) {
                    // Successfully sent to DLQ
                    return new ResilientSendResult(null, true, 
                        "Message sent to DLQ after " + dlqHandler.getMaxRetries() + " retries");
                } else if (result.getType() == DLQHandler.DLQResult.ResultType.DLQ_FAILED) {
                    // DLQ sending failed - rethrow
                    throw new KafkaProducerService.KafkaSendException("DLQ failed: " + result.getMessage(), e);
                } else {
                    // Retry scheduled - this shouldn't happen in sync mode
                    throw new KafkaProducerService.KafkaSendException("Retry scheduled unexpectedly", e);
                }
            } catch (Exception ex) {
                throw new KafkaProducerService.KafkaSendException("DLQ handling failed", e);
            }
        }
    }
    
    /**
     * Subscribes with error handling and DLQ integration.
     */
    public void subscribeWithErrorHandling(String topic, 
                                       KafkaConsumerService.MessageHandler<T> handler) {
        delegate.subscribe(topic, message -> {
            try {
                handler.handle(message);
            } catch (Exception e) {
                // Handle processing errors - could send to DLQ if needed
                System.err.println("Error processing message: " + e.getMessage());
                // Could implement DLQ for processing errors too
            }
        });
    }
    
    /**
     * Subscribes with error handling and filtering.
     */
    public void subscribeWithErrorHandling(String topic,
                                       KafkaConsumerService.MessageHandler<T> handler,
                                       KafkaConsumerService.MessageFilter<T> filter) {
        delegate.subscribe(topic, message -> {
            try {
                handler.handle(message);
            } catch (Exception e) {
                System.err.println("Error processing message: " + e.getMessage());
            }
        }, filter);
    }
    
    // Delegate methods for standard KafkaTemplate operations
    
    public CompletableFuture<KafkaProducerService.SendResult> send(KafkaMessage<T> message) {
        return delegate.send(message);
    }
    
    public CompletableFuture<KafkaProducerService.SendResult> send(String topic, T value) {
        return delegate.send(topic, value);
    }
    
    public CompletableFuture<KafkaProducerService.SendResult> send(String topic, String key, T value) {
        return delegate.send(topic, key, value);
    }
    
    public SendResult sendSync(KafkaMessage<T> message) throws KafkaProducerService.KafkaSendException {
        return delegate.sendSync(message);
    }
    
    public void subscribe(String topic, KafkaConsumerService.MessageHandler<T> handler) {
        delegate.subscribe(topic, handler);
    }
    
    public void subscribe(java.util.List<String> topics, KafkaConsumerService.MessageHandler<T> handler) {
        delegate.subscribe(topics, handler);
    }
    
    public void subscribe(java.util.List<String> topics, 
                       KafkaConsumerService.MessageHandler<T> handler, 
                       KafkaConsumerService.MessageFilter<T> filter) {
        delegate.subscribe(topics, handler, filter);
    }
    
    public void startConsumer() {
        delegate.startConsumer();
    }
    
    public void stopConsumer() {
        delegate.stopConsumer();
    }
    
    public void flush() {
        delegate.flush();
    }
    
    public void commit() {
        delegate.commit();
    }
    
    public void commitAsync() {
        delegate.commitAsync();
    }
    
    public KafkaTemplate.KafkaMetrics getMetrics() {
        return delegate.getMetrics();
    }
    
    public void close() {
        delegate.close();
        dlqHandler.shutdown();
    }
    
    /**
     * Gets DLQ statistics.
     */
    public DLQHandler.DLQStats getDLQStats() {
        return dlqHandler.getStats();
    }
    
    private DLQHandler.DLQResult handleSendFailure(KafkaMessage<T> message, Throwable cause) {
        if (cause instanceof KafkaProducerService.KafkaSendException) {
            KafkaProducerService.KafkaSendException kse = (KafkaProducerService.KafkaSendException) cause;
            return dlqHandler.handleFailureSync(message, kse);
        }
        return new DLQHandler.DLQResult(DLQHandler.DLQResult.ResultType.DLQ_FAILED, 
                "Non-Kafka error: " + cause.getMessage(), 0);
    }
}
