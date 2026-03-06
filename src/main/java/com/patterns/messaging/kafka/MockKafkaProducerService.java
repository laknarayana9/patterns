package com.patterns.messaging.kafka;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple mock Kafka producer for testing.
 */
public class MockKafkaProducerService<T> implements KafkaProducerService<T> {
    
    private final AtomicLong messagesSent = new AtomicLong(0);
    private volatile boolean simulateFailure = false;
    private volatile boolean closed = false;
    private KafkaMessage<T> lastSentMessage;
    
    @Override
    public CompletableFuture<SendResult> sendAsync(KafkaMessage<T> message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return sendSync(message);
            } catch (KafkaSendException e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    @Override
    public SendResult sendSync(KafkaMessage<T> message) throws KafkaSendException {
        if (simulateFailure) {
            throw new KafkaSendException("Simulated failure");
        }
        
        messagesSent.incrementAndGet();
        
        lastSentMessage = message;
        
        return new SendResult(
            message.getTopic(),
            message.getPartition() != null ? message.getPartition() : 0,
            System.currentTimeMillis(),
            message.getMessageId(),
            message.getTimestamp()
        );
    }
    
    @Override
    public CompletableFuture<SendResult> sendAsync(String topic, String key, T value) {
        return sendAsync(KafkaMessage.builder(topic, value).key(key).build());
    }
    
    @Override
    public CompletableFuture<SendResult> sendAsync(String topic, T value) {
        return sendAsync(topic, null, value);
    }
    
    @Override
    public void flush() {
        // Mock - no action needed
    }
    
    @Override
    public void close() {
        closed = true;
    }
    
    @Override
    public ProducerMetrics getMetrics() {
        return new ProducerMetrics() {
            @Override
            public long getMessagesSent() {
                return messagesSent.get();
            }
            
            @Override
            public long getMessagesFailed() {
                return simulateFailure ? 1 : 0;
            }
            
            @Override
            public double getAverageSendTime() {
                return 0.0;
            }
            
            @Override
            public long getBytesSent() {
                return 0;
            }
            
            @Override
            public int getBufferSize() {
                return 0;
            }
            
            @Override
            public boolean isHealthy() {
                return !simulateFailure && !closed;
            }
        };
    }
    
    // Testing utilities
    public void simulateFailure(boolean simulateFailure) {
        this.simulateFailure = simulateFailure;
    }
    
    public long getMessagesSent() {
        return messagesSent.get();
    }
    
    public long getSentMessageCount() {
        return messagesSent.get();
    }
    
    public boolean isClosed() {
        return closed;
    }
    
    public KafkaMessage<T> getSentMessage(String messageId) {
        return lastSentMessage != null && lastSentMessage.getMessageId().equals(messageId) 
            ? lastSentMessage : null;
    }
    
    public void clearSentMessages() {
        messagesSent.set(0);
        lastSentMessage = null;
    }
}
