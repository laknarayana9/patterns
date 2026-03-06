package com.patterns.messaging.kafka;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock implementation of KafkaProducerService for testing.
 * Provides in-memory simulation of Kafka producer behavior.
 */
public class MockKafkaProducerService<T> implements KafkaProducerService<T> {
    
    private final ConcurrentHashMap<String, KafkaMessage<T>> sentMessages = new ConcurrentHashMap<>();
    private final AtomicLong messagesSent = new AtomicLong(0);
    private final AtomicLong messagesFailed = new AtomicLong(0);
    private final AtomicLong totalSendTime = new AtomicLong(0);
    private final AtomicLong bytesSent = new AtomicLong(0);
    private volatile boolean simulateFailure = false;
    private volatile boolean closed = false;
    
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
        if (closed) {
            throw new KafkaSendException("Producer is closed");
        }
        
        long startTime = System.currentTimeMillis();
        
        try {
            if (simulateFailure) {
                messagesFailed.incrementAndGet();
                throw new KafkaSendException("Simulated send failure");
            }
            
            // Store the message for testing purposes
            sentMessages.put(message.getMessageId(), message);
            messagesSent.incrementAndGet();
            
            // Calculate approximate message size
            long messageSize = estimateMessageSize(message);
            bytesSent.addAndGet(messageSize);
            
            long sendTime = System.currentTimeMillis() - startTime;
            totalSendTime.addAndGet(sendTime);
            
            // Return a mock send result
            return new SendResult(
                message.getTopic(),
                message.getPartition() != null ? message.getPartition() : 0,
                System.currentTimeMillis(), // Mock offset
                message.getMessageId(),
                message.getTimestamp()
            );
        } catch (Exception e) {
            messagesFailed.incrementAndGet();
            throw new KafkaSendException("Failed to send message", e);
        }
    }
    
    @Override
    public CompletableFuture<SendResult> sendAsync(String topic, String key, T value) {
        KafkaMessage<T> message = KafkaMessage.builder(topic, value).key(key).build();
        return sendAsync(message);
    }
    
    @Override
    public CompletableFuture<SendResult> sendAsync(String topic, T value) {
        return sendAsync(topic, null, value);
    }
    
    @Override
    public void flush() {
        // Mock implementation - no actual flushing needed
    }
    
    @Override
    public void close() {
        closed = true;
        sentMessages.clear();
    }
    
    @Override
    public ProducerMetrics getMetrics() {
        return new MockProducerMetrics();
    }
    
    // Testing utilities
    
    public void simulateFailure(boolean simulateFailure) {
        this.simulateFailure = simulateFailure;
    }
    
    public KafkaMessage<T> getSentMessage(String messageId) {
        return sentMessages.get(messageId);
    }
    
    public int getSentMessageCount() {
        return sentMessages.size();
    }
    
    public void clearSentMessages() {
        sentMessages.clear();
    }
    
    public boolean isClosed() {
        return closed;
    }
    
    private long estimateMessageSize(KafkaMessage<T> message) {
        long size = 0;
        if (message.getKey() != null) {
            size += message.getKey().getBytes().length;
        }
        if (message.getValue() != null) {
            size += message.getValue().toString().getBytes().length;
        }
        size += message.getTopic().getBytes().length;
        size += message.getHeaders().values().stream()
                .mapToInt(h -> h.getBytes().length)
                .sum();
        return size;
    }
    
    private class MockProducerMetrics implements ProducerMetrics {
        @Override
        public long getMessagesSent() {
            return messagesSent.get();
        }
        
        @Override
        public long getMessagesFailed() {
            return messagesFailed.get();
        }
        
        @Override
        public double getAverageSendTime() {
            long sent = messagesSent.get();
            return sent > 0 ? (double) totalSendTime.get() / sent : 0.0;
        }
        
        @Override
        public long getBytesSent() {
            return bytesSent.get();
        }
        
        @Override
        public int getBufferSize() {
            return 0; // Mock implementation
        }
        
        @Override
        public boolean isHealthy() {
            return !closed && messagesFailed.get() < messagesSent.get();
        }
        
        @Override
        public String toString() {
            return String.format("MockProducerMetrics{sent=%d, failed=%d, avgTime=%.2fms, bytes=%d}",
                    getMessagesSent(), getMessagesFailed(), getAverageSendTime(), getBytesSent());
        }
    }
}
