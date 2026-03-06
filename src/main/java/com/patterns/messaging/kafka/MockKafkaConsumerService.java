package com.patterns.messaging.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock implementation of KafkaConsumerService for testing.
 * Provides in-memory simulation of Kafka consumer behavior.
 */
public class MockKafkaConsumerService<T> implements KafkaConsumerService<T> {
    
    private final BlockingQueue<ConsumerRecord<T>> messageQueue = new LinkedBlockingQueue<>();
    private final List<String> subscribedTopics = new CopyOnWriteArrayList<>();
    private final AtomicLong messagesConsumed = new AtomicLong(0);
    private final AtomicLong messagesFailed = new AtomicLong(0);
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    private final AtomicLong currentLag = new AtomicLong(0);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private volatile MessageHandler<T> messageHandler;
    private volatile MessageFilter<T> messageFilter;
    private volatile Thread consumerThread;
    private volatile boolean closed = false;
    
    @Override
    public void subscribe(List<String> topics, MessageHandler<T> handler) {
        subscribe(topics, handler, null);
    }
    
    @Override
    public void subscribe(String topic, MessageHandler<T> handler) {
        subscribe(List.of(topic), handler);
    }
    
    @Override
    public void subscribe(List<String> topics, MessageHandler<T> handler, MessageFilter<T> filter) {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        
        this.subscribedTopics.clear();
        this.subscribedTopics.addAll(topics);
        this.messageHandler = handler;
        this.messageFilter = filter;
    }
    
    @Override
    public List<ConsumerRecord<T>> poll(long timeout) {
        List<ConsumerRecord<T>> records = new ArrayList<>();
        long endTime = System.currentTimeMillis() + timeout;
        
        while (System.currentTimeMillis() < endTime && records.isEmpty()) {
            try {
                ConsumerRecord<T> record = messageQueue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (record != null) {
                    records.add(record);
                    messagesConsumed.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        return records;
    }
    
    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(this::consumeMessages, "MockKafkaConsumer");
            consumerThread.setDaemon(true);
            consumerThread.start();
        }
    }
    
    @Override
    public void stop() {
        running.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    @Override
    public void commit() {
        // Mock implementation - no actual committing needed
    }
    
    @Override
    public void commitAsync() {
        // Mock implementation - no actual committing needed
    }
    
    @Override
    public void seek(String topic, int partition, long offset) {
        // Mock implementation - no actual seeking needed
    }
    
    @Override
    public void pause() {
        paused.set(true);
    }
    
    @Override
    public void resume() {
        paused.set(false);
    }
    
    @Override
    public ConsumerMetrics getMetrics() {
        return new MockConsumerMetrics();
    }
    
    @Override
    public void close() {
        stop();
        closed = true;
        messageQueue.clear();
        subscribedTopics.clear();
    }
    
    // Testing utilities
    
    public void addMessage(KafkaMessage<T> message) {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        
        ConsumerRecord<T> record = new ConsumerRecord<>(
            message.getTopic(),
            message.getPartition() != null ? message.getPartition() : 0,
            System.currentTimeMillis(), // Mock offset
            message.getKey(),
            message.getValue(),
            message.getTimestamp(),
            message.getMessageId(),
            message.getCorrelationId()
        );
        
        messageQueue.offer(record);
        currentLag.incrementAndGet();
    }
    
    public void addMessage(String topic, T value) {
        addMessage(KafkaMessage.builder(topic, value).build());
    }
    
    public void addMessage(String topic, String key, T value) {
        addMessage(KafkaMessage.builder(topic, value).key(key).build());
    }
    
    public int getQueueSize() {
        return messageQueue.size();
    }
    
    public List<String> getSubscribedTopics() {
        return new ArrayList<>(subscribedTopics);
    }
    
    public boolean isRunning() {
        return running.get();
    }
    
    public boolean isPaused() {
        return paused.get();
    }
    
    public boolean isClosed() {
        return closed;
    }
    
    private void consumeMessages() {
        while (running.get() && !closed) {
            if (!paused.get()) {
                try {
                    ConsumerRecord<T> record = messageQueue.take();
                    currentLag.decrementAndGet();
                    
                    if (messageHandler != null) {
                        long startTime = System.currentTimeMillis();
                        
                        try {
                            KafkaMessage<T> kafkaMessage = record.toKafkaMessage();
                            
                            // Apply filter if present
                            if (messageFilter == null || messageFilter.accept(kafkaMessage)) {
                                messageHandler.handle(kafkaMessage);
                            }
                            
                            long processingTime = System.currentTimeMillis() - startTime;
                            totalProcessingTime.addAndGet(processingTime);
                            
                        } catch (Exception e) {
                            messagesFailed.incrementAndGet();
                            // Log error but continue processing
                            System.err.println("Error processing message: " + e.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } else {
                try {
                    Thread.sleep(100); // Sleep when paused
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
    
    private class MockConsumerMetrics implements ConsumerMetrics {
        @Override
        public long getMessagesConsumed() {
            return messagesConsumed.get();
        }
        
        @Override
        public long getMessagesFailed() {
            return messagesFailed.get();
        }
        
        @Override
        public double getAverageProcessingTime() {
            long consumed = messagesConsumed.get();
            return consumed > 0 ? (double) totalProcessingTime.get() / consumed : 0.0;
        }
        
        @Override
        public long getCurrentLag() {
            return currentLag.get();
        }
        
        @Override
        public int getActivePartitions() {
            return subscribedTopics.size(); // Mock implementation
        }
        
        @Override
        public boolean isHealthy() {
            return !closed && running.get() && messagesFailed.get() < messagesConsumed.get();
        }
        
        @Override
        public String toString() {
            return String.format("MockConsumerMetrics{consumed=%d, failed=%d, avgTime=%.2fms, lag=%d, partitions=%d}",
                    getMessagesConsumed(), getMessagesFailed(), getAverageProcessingTime(), 
                    getCurrentLag(), getActivePartitions());
        }
    }
}
