package com.patterns.messaging.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterEach;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class MockKafkaProducerServiceTest {
    
    private MockKafkaProducerService<String> producer;
    private static final String TOPIC = "test-topic";
    private static final String KEY = "test-key";
    private static final String VALUE = "test-value";
    
    @BeforeEach
    void setUp() {
        producer = new MockKafkaProducerService<>();
    }
    
    @AfterEach
    void tearDown() {
        if (producer != null && !producer.isClosed()) {
            producer.close();
        }
    }
    
    @Test
    @DisplayName("Should send message synchronously successfully")
    void shouldSendMessageSynchronouslySuccessfully() throws KafkaProducerService.KafkaSendException {
        KafkaMessage<String> message = KafkaMessage.of(KEY, TOPIC, VALUE);
        
        KafkaProducerService.SendResult result = producer.sendSync(message);
        
        assertNotNull(result);
        assertEquals(TOPIC, result.getTopic());
        assertEquals(message.getMessageId(), result.getMessageId());
        assertEquals(1, producer.getSentMessageCount());
        assertNotNull(producer.getSentMessage(message.getMessageId()));
    }
    
    @Test
    @DisplayName("Should send message asynchronously successfully")
    void shouldSendMessageAsynchronouslySuccessfully() throws Exception {
        KafkaMessage<String> message = KafkaMessage.of(KEY, TOPIC, VALUE);
        
        CompletableFuture<KafkaProducerService.SendResult> future = producer.sendAsync(message);
        KafkaProducerService.SendResult result = future.get();
        
        assertNotNull(result);
        assertEquals(TOPIC, result.getTopic());
        assertEquals(message.getMessageId(), result.getMessageId());
        assertEquals(1, producer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should send message with topic and value")
    void shouldSendMessageWithTopicAndValue() throws Exception {
        CompletableFuture<KafkaProducerService.SendResult> future = producer.sendAsync(TOPIC, VALUE);
        KafkaProducerService.SendResult result = future.get();
        
        assertNotNull(result);
        assertEquals(TOPIC, result.getTopic());
        assertEquals(1, producer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should send message with topic, key, and value")
    void shouldSendMessageWithTopicKeyAndValue() throws Exception {
        CompletableFuture<KafkaProducerService.SendResult> future = producer.sendAsync(TOPIC, KEY, VALUE);
        KafkaProducerService.SendResult result = future.get();
        
        assertNotNull(result);
        assertEquals(TOPIC, result.getTopic());
        assertEquals(1, producer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should handle simulated failure")
    void shouldHandleSimulatedFailure() {
        producer.simulateFailure(true);
        KafkaMessage<String> message = KafkaMessage.of(KEY, TOPIC, VALUE);
        
        assertThrows(KafkaProducerService.KafkaSendException.class, () -> 
            producer.sendSync(message));
        
        assertEquals(0, producer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should handle async failure")
    void shouldHandleAsyncFailure() {
        producer.simulateFailure(true);
        KafkaMessage<String> message = KafkaMessage.of(KEY, TOPIC, VALUE);
        
        CompletableFuture<KafkaProducerService.SendResult> future = producer.sendAsync(message);
        
        assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(0, producer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should throw exception when sending to closed producer")
    void shouldThrowExceptionWhenSendingToClosedProducer() {
        producer.close();
        KafkaMessage<String> message = KafkaMessage.of(KEY, TOPIC, VALUE);
        
        assertThrows(KafkaProducerService.KafkaSendException.class, () -> 
            producer.sendSync(message));
    }
    
    @Test
    @DisplayName("Should track metrics correctly")
    void shouldTrackMetricsCorrectly() throws KafkaProducerService.KafkaSendException {
        // Send some messages
        producer.sendSync(KafkaMessage.of(TOPIC, "message1"));
        producer.sendSync(KafkaMessage.of(TOPIC, "message2"));
        
        // Simulate one failure
        producer.simulateFailure(true);
        try {
            producer.sendSync(KafkaMessage.of(TOPIC, "message3"));
        } catch (KafkaProducerService.KafkaSendException e) {
            // Expected
        }
        
        KafkaProducerService.ProducerMetrics metrics = producer.getMetrics();
        
        assertEquals(2, metrics.getMessagesSent());
        assertEquals(1, metrics.getMessagesFailed());
        assertTrue(metrics.getAverageSendTime() >= 0);
        assertTrue(metrics.getBytesSent() > 0);
        assertTrue(metrics.isHealthy());
    }
    
    @Test
    @DisplayName("Should clear sent messages")
    void shouldClearSentMessages() throws KafkaProducerService.KafkaSendException {
        producer.sendSync(KafkaMessage.of(TOPIC, VALUE));
        assertEquals(1, producer.getSentMessageCount());
        
        producer.clearSentMessages();
        assertEquals(0, producer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should handle flush operation")
    void shouldHandleFlushOperation() {
        assertDoesNotThrow(() -> producer.flush());
    }
    
    @Test
    @DisplayName("Should close producer properly")
    void shouldCloseProducerProperly() {
        assertFalse(producer.isClosed());
        
        producer.close();
        
        assertTrue(producer.isClosed());
        assertEquals(0, producer.getSentMessageCount());
    }
    
    @Test
    @DisplayName("Should provide meaningful metrics toString")
    void shouldProvideMeaningfulMetricsToString() throws KafkaProducerService.KafkaSendException {
        producer.sendSync(KafkaMessage.of(TOPIC, VALUE));
        
        String metricsString = producer.getMetrics().toString();
        
        assertTrue(metricsString.contains("sent=1"));
        assertTrue(metricsString.contains("failed=0"));
        assertTrue(metricsString.contains("MockProducerMetrics"));
    }
}
