package com.patterns.messaging.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterEach;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class KafkaTemplateTest {
    
    private MockKafkaProducerService<String> mockProducer;
    private MockKafkaConsumerService<String> mockConsumer;
    private KafkaTemplate<String> kafkaTemplate;
    
    private static final String TOPIC = "test-topic";
    private static final String KEY = "test-key";
    private static final String VALUE = "test-value";
    
    @BeforeEach
    void setUp() {
        mockProducer = new MockKafkaProducerService<>();
        mockConsumer = new MockKafkaConsumerService<>();
        kafkaTemplate = new KafkaTemplate<>(mockProducer, mockConsumer);
    }
    
    @AfterEach
    void tearDown() {
        if (kafkaTemplate != null) {
            kafkaTemplate.close();
        }
    }
    
    @Test
    @DisplayName("Should send message using KafkaMessage")
    void shouldSendMessageUsingKafkaMessage() throws Exception {
        KafkaMessage<String> message = KafkaMessage.of(KEY, TOPIC, VALUE);
        
        CompletableFuture<KafkaProducerService.SendResult> future = kafkaTemplate.send(message);
        KafkaProducerService.SendResult result = future.get();
        
        assertNotNull(result);
        assertEquals(TOPIC, result.getTopic());
        assertEquals(1, mockProducer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should send message with topic and value")
    void shouldSendMessageWithTopicAndValue() throws Exception {
        CompletableFuture<KafkaProducerService.SendResult> future = kafkaTemplate.send(TOPIC, VALUE);
        KafkaProducerService.SendResult result = future.get();
        
        assertNotNull(result);
        assertEquals(TOPIC, result.getTopic());
        assertEquals(1, mockProducer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should send message with topic, key, and value")
    void shouldSendMessageWithTopicKeyAndValue() throws Exception {
        CompletableFuture<KafkaProducerService.SendResult> future = kafkaTemplate.send(TOPIC, KEY, VALUE);
        KafkaProducerService.SendResult result = future.get();
        
        assertNotNull(result);
        assertEquals(TOPIC, result.getTopic());
        assertEquals(1, mockProducer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should send message synchronously")
    void shouldSendMessageSynchronously() throws KafkaProducerService.KafkaSendException {
        KafkaMessage<String> message = KafkaMessage.of(KEY, TOPIC, VALUE);
        
        KafkaProducerService.SendResult result = kafkaTemplate.sendSync(message);
        
        assertNotNull(result);
        assertEquals(TOPIC, result.getTopic());
        assertEquals(1, mockProducer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should subscribe to single topic")
    void shouldSubscribeToSingleTopic() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger receivedMessages = new AtomicInteger(0);
        
        kafkaTemplate.subscribe(TOPIC, message -> {
            receivedMessages.incrementAndGet();
            latch.countDown();
        });
        
        kafkaTemplate.startConsumer();
        mockConsumer.addMessage(TOPIC, VALUE);
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, receivedMessages.get());
    }
    
    @Test
    @DisplayName("Should subscribe to multiple topics")
    void shouldSubscribeToMultipleTopics() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger receivedMessages = new AtomicInteger(0);
        
        kafkaTemplate.subscribe(List.of("topic1", "topic2"), message -> {
            receivedMessages.incrementAndGet();
            latch.countDown();
        });
        
        kafkaTemplate.startConsumer();
        mockConsumer.addMessage("topic1", "message1");
        mockConsumer.addMessage("topic2", "message2");
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, receivedMessages.get());
    }
    
    @Test
    @DisplayName("Should subscribe with filter")
    void shouldSubscribeWithFilter() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger receivedMessages = new AtomicInteger(0);
        
        kafkaTemplate.subscribe(List.of(TOPIC), 
            message -> {
                receivedMessages.incrementAndGet();
                latch.countDown();
            },
            message -> VALUE.equals(message.getValue())
        );
        
        kafkaTemplate.startConsumer();
        mockConsumer.addMessage(TOPIC, VALUE); // Should pass filter
        mockConsumer.addMessage(TOPIC, "other-value"); // Should be filtered out
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, receivedMessages.get());
    }
    
    @Test
    @DisplayName("Should implement send and receive pattern")
    void shouldImplementSendAndReceivePattern() throws Exception {
        String correlationId = "test-correlation";
        String requestTopic = "request-topic";
        String replyTopic = "reply-topic";
        String requestValue = "request";
        String replyValue = "reply";
        
        CompletableFuture<String> responseFuture = kafkaTemplate.sendAndReceive(
            requestTopic, replyTopic, requestValue, correlationId, 5000
        );
        
        // Simulate reply
        kafkaTemplate.startConsumer();
        KafkaMessage<String> replyMessage = KafkaMessage.builder(replyTopic, replyValue)
                .correlationId(correlationId)
                .build();
        mockConsumer.addMessage(replyMessage);
        
        String response = responseFuture.get(5, TimeUnit.SECONDS);
        
        assertEquals(replyValue, response);
    }
    
    @Test
    @DisplayName("Should handle send and receive timeout")
    void shouldHandleSendAndReceiveTimeout() throws Exception {
        String correlationId = "test-correlation";
        String requestTopic = "request-topic";
        String replyTopic = "reply-topic";
        String requestValue = "request";
        
        CompletableFuture<String> responseFuture = kafkaTemplate.sendAndReceive(
            requestTopic, replyTopic, requestValue, correlationId, 100 // Short timeout
        );
        
        // Don't send reply, should timeout
        
        assertThrows(java.util.concurrent.TimeoutException.class, () -> 
            responseFuture.get(1, TimeUnit.SECONDS));
    }
    
    @Test
    @DisplayName("Should start and stop consumer")
    void shouldStartAndStopConsumer() {
        assertFalse(mockConsumer.isRunning());
        
        kafkaTemplate.startConsumer();
        assertTrue(mockConsumer.isRunning());
        
        kafkaTemplate.stopConsumer();
        assertFalse(mockConsumer.isRunning());
    }
    
    @Test
    @DisplayName("Should handle flush operation")
    void shouldHandleFlushOperation() {
        assertDoesNotThrow(() -> kafkaTemplate.flush());
    }
    
    @Test
    @DisplayName("Should handle commit operations")
    void shouldHandleCommitOperations() {
        assertDoesNotThrow(() -> kafkaTemplate.commit());
        assertDoesNotThrow(() -> kafkaTemplate.commitAsync());
    }
    
    @Test
    @DisplayName("Should provide combined metrics")
    void shouldProvideCombinedMetrics() throws KafkaProducerService.KafkaSendException {
        // Send a message to generate producer metrics
        kafkaTemplate.send(TOPIC, VALUE);
        
        // Start consumer and add message to generate consumer metrics
        kafkaTemplate.subscribe(TOPIC, message -> {});
        kafkaTemplate.startConsumer();
        mockConsumer.addMessage(TOPIC, VALUE);
        
        // Wait a bit for processing
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        KafkaTemplate.KafkaMetrics metrics = kafkaTemplate.getMetrics();
        
        assertNotNull(metrics);
        assertNotNull(metrics.getProducerMetrics());
        assertNotNull(metrics.getConsumerMetrics());
        assertTrue(metrics.isHealthy());
        
        assertEquals(1, metrics.getProducerMetrics().getMessagesSent());
        assertTrue(metrics.getConsumerMetrics().getMessagesConsumed() >= 0);
    }
    
    @Test
    @DisplayName("Should close both producer and consumer")
    void shouldCloseBothProducerAndConsumer() {
        kafkaTemplate.startConsumer();
        
        assertFalse(mockProducer.isClosed());
        assertFalse(mockConsumer.isClosed());
        
        kafkaTemplate.close();
        
        assertTrue(mockProducer.isClosed());
        assertTrue(mockConsumer.isClosed());
    }
    
    @Test
    @DisplayName("Should provide meaningful metrics toString")
    void shouldProvideMeaningfulMetricsToString() throws KafkaProducerService.KafkaSendException {
        kafkaTemplate.send(TOPIC, VALUE);
        
        String metricsString = kafkaTemplate.getMetrics().toString();
        
        assertTrue(metricsString.contains("KafkaMetrics"));
        assertTrue(metricsString.contains("producer="));
        assertTrue(metricsString.contains("consumer="));
        assertTrue(metricsString.contains("healthy="));
    }
}
