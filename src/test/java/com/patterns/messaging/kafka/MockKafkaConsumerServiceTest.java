package com.patterns.messaging.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterEach;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MockKafkaConsumerServiceTest {
    
    private MockKafkaConsumerService<String> consumer;
    private static final String TOPIC = "test-topic";
    private static final String KEY = "test-key";
    private static final String VALUE = "test-value";
    
    @BeforeEach
    void setUp() {
        consumer = new MockKafkaConsumerService<>();
    }
    
    @AfterEach
    void tearDown() {
        if (consumer != null && !consumer.isClosed()) {
            consumer.close();
        }
    }
    
    @Test
    @DisplayName("Should subscribe to topics and receive messages")
    void shouldSubscribeToTopicsAndReceiveMessages() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger receivedMessages = new AtomicInteger(0);
        
        consumer.subscribe(TOPIC, message -> {
            receivedMessages.incrementAndGet();
            latch.countDown();
        });
        
        consumer.start();
        consumer.addMessage(TOPIC, VALUE);
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, receivedMessages.get());
    }
    
    @Test
    @DisplayName("Should filter messages based on filter")
    void shouldFilterMessagesBasedOnFilter() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger receivedMessages = new AtomicInteger(0);
        
        consumer.subscribe(List.of(TOPIC), 
            message -> {
                receivedMessages.incrementAndGet();
                latch.countDown();
            },
            message -> VALUE.equals(message.getValue())
        );
        
        consumer.start();
        consumer.addMessage(TOPIC, VALUE); // Should pass filter
        consumer.addMessage(TOPIC, "other-value"); // Should be filtered out
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, receivedMessages.get());
    }
    
    @Test
    @DisplayName("Should poll messages manually")
    void shouldPollMessagesManually() {
        consumer.addMessage(TOPIC, VALUE);
        consumer.addMessage(TOPIC, "value2");
        
        List<KafkaConsumerService.ConsumerRecord<String>> records = consumer.poll(1000);
        
        assertEquals(1, records.size()); // Should get one message per poll
        assertEquals(VALUE, records.get(0).getValue());
    }
    
    @Test
    @DisplayName("Should handle message with key")
    void shouldHandleMessageWithKey() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger receivedMessages = new AtomicInteger(0);
        
        consumer.subscribe(TOPIC, message -> {
            receivedMessages.incrementAndGet();
            assertEquals(KEY, message.getKey());
            latch.countDown();
        });
        
        consumer.start();
        consumer.addMessage(TOPIC, KEY, VALUE);
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, receivedMessages.get());
    }
    
    @Test
    @DisplayName("Should pause and resume consumption")
    void shouldPauseAndResumeConsumption() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger receivedMessages = new AtomicInteger(0);
        
        consumer.subscribe(TOPIC, message -> {
            receivedMessages.incrementAndGet();
            latch.countDown();
        });
        
        consumer.start();
        
        // Add first message (should be processed)
        consumer.addMessage(TOPIC, "message1");
        Thread.sleep(100);
        
        // Pause consumption
        consumer.pause();
        
        // Add second message (should not be processed while paused)
        consumer.addMessage(TOPIC, "message2");
        Thread.sleep(100);
        
        // Resume consumption
        consumer.resume();
        
        // Add third message (should be processed after resume)
        consumer.addMessage(TOPIC, "message3");
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, receivedMessages.get()); // message1 and message3
    }
    
    @Test
    @DisplayName("Should track metrics correctly")
    void shouldTrackMetricsCorrectly() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        
        consumer.subscribe(TOPIC, message -> {
            latch.countDown();
        });
        
        consumer.start();
        consumer.addMessage(TOPIC, "message1");
        consumer.addMessage(TOPIC, "message2");
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        
        KafkaConsumerService.ConsumerMetrics metrics = consumer.getMetrics();
        
        assertEquals(2, metrics.getMessagesConsumed());
        assertEquals(0, metrics.getMessagesFailed());
        assertTrue(metrics.getAverageProcessingTime() >= 0);
        assertTrue(metrics.isHealthy());
    }
    
    @Test
    @DisplayName("Should handle message processing errors")
    void shouldHandleMessageProcessingErrors() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger processedMessages = new AtomicInteger(0);
        
        consumer.subscribe(TOPIC, message -> {
            processedMessages.incrementAndGet();
            latch.countDown();
            
            // Throw exception on second message
            if (message.getValue().equals("error-message")) {
                throw new RuntimeException("Processing error");
            }
        });
        
        consumer.start();
        consumer.addMessage(TOPIC, "good-message");
        consumer.addMessage(TOPIC, "error-message");
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        
        KafkaConsumerService.ConsumerMetrics metrics = consumer.getMetrics();
        assertEquals(1, metrics.getMessagesFailed());
        assertTrue(metrics.isHealthy()); // Still healthy because failures < successes
    }
    
    @Test
    @DisplayName("Should stop and start consumer")
    void shouldStopAndStartConsumer() throws InterruptedException {
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        
        consumer.subscribe(TOPIC, message -> {
            if (message.getValue().equals("message1")) {
                latch1.countDown();
            } else if (message.getValue().equals("message2")) {
                latch2.countDown();
            }
        });
        
        consumer.start();
        consumer.addMessage(TOPIC, "message1");
        
        assertTrue(latch1.await(5, TimeUnit.SECONDS));
        
        consumer.stop();
        consumer.addMessage(TOPIC, "message2"); // Should not be processed
        
        assertFalse(latch2.await(1, TimeUnit.SECONDS));
        
        consumer.start();
        consumer.addMessage(TOPIC, "message3"); // Should be processed
        
        // Wait a bit for processing
        Thread.sleep(100);
        
        KafkaConsumerService.ConsumerMetrics metrics = consumer.getMetrics();
        assertTrue(metrics.getMessagesConsumed() >= 1);
    }
    
    @Test
    @DisplayName("Should handle commit operations")
    void shouldHandleCommitOperations() {
        assertDoesNotThrow(() -> consumer.commit());
        assertDoesNotThrow(() -> consumer.commitAsync());
    }
    
    @Test
    @DisplayName("Should handle seek operation")
    void shouldHandleSeekOperation() {
        assertDoesNotThrow(() -> consumer.seek(TOPIC, 0, 100));
    }
    
    @Test
    @DisplayName("Should close consumer properly")
    void shouldCloseConsumerProperly() {
        consumer.start();
        assertFalse(consumer.isClosed());
        
        consumer.close();
        
        assertTrue(consumer.isClosed());
        assertFalse(consumer.isRunning());
        assertEquals(0, consumer.getQueueSize());
    }
    
    @Test
    @DisplayName("Should throw exception when adding message to closed consumer")
    void shouldThrowExceptionWhenAddingMessageToClosedConsumer() {
        consumer.close();
        
        assertThrows(IllegalStateException.class, () -> 
            consumer.addMessage(TOPIC, VALUE));
    }
    
    @Test
    @DisplayName("Should provide queue size information")
    void shouldProvideQueueSizeInformation() {
        assertEquals(0, consumer.getQueueSize());
        
        consumer.addMessage(TOPIC, VALUE);
        assertEquals(1, consumer.getQueueSize());
        
        consumer.poll(1000); // Should remove one message
        assertEquals(0, consumer.getQueueSize());
    }
    
    @Test
    @DisplayName("Should track subscribed topics")
    void shouldTrackSubscribedTopics() {
        consumer.subscribe(List.of("topic1", "topic2"), message -> {});
        
        List<String> topics = consumer.getSubscribedTopics();
        assertEquals(2, topics.size());
        assertTrue(topics.contains("topic1"));
        assertTrue(topics.contains("topic2"));
    }
    
    @Test
    @DisplayName("Should provide meaningful metrics toString")
    void shouldProvideMeaningfulMetricsToString() throws InterruptedException {
        consumer.subscribe(TOPIC, message -> {});
        consumer.start();
        consumer.addMessage(TOPIC, VALUE);
        
        Thread.sleep(100); // Allow processing
        
        String metricsString = consumer.getMetrics().toString();
        
        assertTrue(metricsString.contains("MockConsumerMetrics"));
        assertTrue(metricsString.contains("consumed="));
        assertTrue(metricsString.contains("failed="));
    }
}
