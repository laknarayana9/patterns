package com.patterns.messaging.kafka;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dead Letter Queue (DLQ) handler with exponential backoff.
 * Handles failed messages with retry logic and DLQ routing.
 */
public class DLQHandler<T> {
    
    private final KafkaProducerService<T> dlqProducer;
    private final KafkaProducerService<T> retryProducer;
    private final String dlqTopic;
    private final int maxRetries;
    private final long initialBackoffMs;
    private final double backoffMultiplier;
    private final long maxBackoffMs;
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicLong failedMessages = new AtomicLong(0);
    private final AtomicLong retriedMessages = new AtomicLong(0);
    private final AtomicLong dlqMessages = new AtomicLong(0);
    private final Random random = new Random();
    
    public DLQHandler(KafkaProducerService<T> dlqProducer, 
                     KafkaProducerService<T> retryProducer,
                     String dlqTopic) {
        this(dlqProducer, retryProducer, dlqTopic, 3, 1000, 2.0, 30000);
    }
    
    public DLQHandler(KafkaProducerService<T> dlqProducer,
                     KafkaProducerService<T> retryProducer, 
                     String dlqTopic,
                     int maxRetries,
                     long initialBackoffMs,
                     double backoffMultiplier,
                     long maxBackoffMs) {
        this.dlqProducer = dlqProducer;
        this.retryProducer = retryProducer;
        this.dlqTopic = dlqTopic;
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.backoffMultiplier = backoffMultiplier;
        this.maxBackoffMs = maxBackoffMs;
    }
    
    /**
     * Handles message failure with retry logic and DLQ routing.
     */
    public CompletableFuture<DLQResult> handleFailure(KafkaMessage<T> message, Exception error) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return handleFailureSync(message, error);
            } catch (Exception e) {
                throw new RuntimeException("DLQ handling failed", e);
            }
        });
    }
    
    private DLQResult handleFailureSync(KafkaMessage<T> message, Exception error) {
        failedMessages.incrementAndGet();
        
        // Check if message has retry information
        int currentRetryCount = getRetryCount(message);
        
        if (currentRetryCount < maxRetries) {
            // Retry with exponential backoff
            return retryWithBackoff(message, currentRetryCount);
        } else {
            // Send to DLQ after max retries
            return sendToDLQ(message, error);
        }
    }
    
    private DLQResult retryWithBackoff(KafkaMessage<T> message, int currentRetryCount) {
        long backoffMs = calculateBackoff(currentRetryCount);
        
        // Add retry header
        KafkaMessage<T> retryMessage = message.toBuilder()
                .header("retry-count", String.valueOf(currentRetryCount + 1))
                .header("backoff-ms", String.valueOf(backoffMs))
                .header("original-timestamp", String.valueOf(message.getTimestamp()))
                .build();
        
        // Schedule retry with backoff
        scheduler.schedule(() -> {
            try {
                retryProducer.sendSync(retryMessage);
                retriedMessages.incrementAndGet();
            } catch (Exception e) {
                // Retry failed - handle recursively
                handleFailure(retryMessage, e);
            }
        }, backoffMs, TimeUnit.MILLISECONDS);
        
        return DLQResult.retryScheduled(backoffMs);
    }
    
    private DLQResult sendToDLQ(KafkaMessage<T> message, Exception originalError) {
        try {
            // Add DLQ headers
            KafkaMessage<T> dlqMessage = message.toBuilder()
                    .topic(dlqTopic)
                    .header("dlq-reason", originalError.getClass().getSimpleName())
                    .header("dlq-original-topic", message.getTopic())
                    .header("dlq-timestamp", String.valueOf(System.currentTimeMillis()))
                    .header("failure-reason", originalError.getMessage())
                    .header("max-retries-exceeded", String.valueOf(maxRetries))
                    .build();
            
            dlqProducer.sendSync(dlqMessage);
            dlqMessages.incrementAndGet();
            
            return DLQResult.sentToDLQ();
        } catch (Exception e) {
            return DLQResult.dlqFailed("Failed to send to DLQ: " + e.getMessage());
        }
    }
    
    private long calculateBackoff(int retryCount) {
        // Exponential backoff with jitter: base * multiplier^retry + random jitter
        long exponentialBackoff = (long) (initialBackoffMs * Math.pow(backoffMultiplier, retryCount));
        
        // Add jitter: +/- 25% of backoff to prevent thundering herd
        long jitterRange = (long) (exponentialBackoff * 0.25);
        long jitter = (long) (random.nextDouble() * jitterRange) - (jitterRange / 2);
        
        return Math.min(exponentialBackoff + jitter, maxBackoffMs);
    }
    
    private int getRetryCount(KafkaMessage<T> message) {
        String retryHeader = message.getHeaders().get("retry-count");
        return retryHeader != null ? Integer.parseInt(retryHeader) : 0;
    }
    
    /**
     * Gets DLQ statistics.
     */
    public DLQStats getStats() {
        return new DLQStats(
            failedMessages.get(),
            retriedMessages.get(),
            dlqMessages.get()
        );
    }
    
    /**
     * Gets maximum retry count.
     */
    public int getMaxRetries() {
        return maxRetries;
    }
    
    /**
     * Shuts down the DLQ handler.
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Result of DLQ handling operation.
     */
    public static class DLQResult {
        private final ResultType type;
        private final String message;
        private final long backoffMs;
        
        private DLQResult(ResultType type, String message, long backoffMs) {
            this.type = type;
            this.message = message;
            this.backoffMs = backoffMs;
        }
        
        public static DLQResult retryScheduled(long backoffMs) {
            return new DLQResult(ResultType.RETRY_SCHEDULED, 
                "Retry scheduled with backoff: " + backoffMs + "ms", backoffMs);
        }
        
        public static DLQResult sentToDLQ() {
            return new DLQResult(ResultType.SENT_TO_DLQ, 
                "Message sent to DLQ", 0);
        }
        
        public static DLQResult dlqFailed(String error) {
            return new DLQResult(ResultType.DLQ_FAILED, error, 0);
        }
        
        public ResultType getType() { return type; }
        public String getMessage() { return message; }
        public long getBackoffMs() { return backoffMs; }
        
        public enum ResultType {
            RETRY_SCHEDULED, SENT_TO_DLQ, DLQ_FAILED
        }
    }
    
    /**
     * DLQ statistics.
     */
    public static class DLQStats {
        private final long failedMessages;
        private final long retriedMessages;
        private final long dlqMessages;
        
        public DLQStats(long failedMessages, long retriedMessages, long dlqMessages) {
            this.failedMessages = failedMessages;
            this.retriedMessages = retriedMessages;
            this.dlqMessages = dlqMessages;
        }
        
        public long getFailedMessages() { return failedMessages; }
        public long getRetriedMessages() { return retriedMessages; }
        public long getDlqMessages() { return dlqMessages; }
        
        @Override
        public String toString() {
            return String.format("DLQStats{failed=%d, retried=%d, dlq=%d}", 
                    failedMessages, retriedMessages, dlqMessages);
        }
    }
}
