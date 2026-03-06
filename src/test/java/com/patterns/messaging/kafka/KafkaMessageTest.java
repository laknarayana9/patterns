package com.patterns.messaging.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KafkaMessageTest {
    
    private static final String TOPIC = "test-topic";
    private static final String KEY = "test-key";
    private static final String VALUE = "test-value";
    private static final String MESSAGE_ID = "msg-123";
    private static final String CORRELATION_ID = "corr-456";
    private static final String SOURCE_SYSTEM = "test-system";
    
    @Test
    @DisplayName("Should create message with topic and value")
    void shouldCreateMessageWithTopicAndValue() {
        KafkaMessage<String> message = KafkaMessage.of(TOPIC, VALUE);
        
        assertEquals(TOPIC, message.getTopic());
        assertEquals(VALUE, message.getValue());
        assertNull(message.getKey());
        assertNull(message.getPartition());
        assertNotNull(message.getMessageId());
        assertNull(message.getCorrelationId());
        assertNull(message.getSourceSystem());
        assertTrue(message.getHeaders().isEmpty());
    }
    
    @Test
    @DisplayName("Should create message with key, topic, and value")
    void shouldCreateMessageWithKeyTopicAndValue() {
        KafkaMessage<String> message = KafkaMessage.of(KEY, TOPIC, VALUE);
        
        assertEquals(TOPIC, message.getTopic());
        assertEquals(VALUE, message.getValue());
        assertEquals(KEY, message.getKey());
        assertNotNull(message.getMessageId());
    }
    
    @Test
    @DisplayName("Should build message with all fields using builder")
    void shouldBuildMessageWithAllFields() {
        Map<String, String> headers = Map.of("header1", "value1", "header2", "value2");
        
        KafkaMessage<String> message = KafkaMessage.builder(TOPIC, VALUE)
                .key(KEY)
                .partition(1)
                .timestamp(System.currentTimeMillis())
                .headers(headers)
                .messageId(MESSAGE_ID)
                .correlationId(CORRELATION_ID)
                .sourceSystem(SOURCE_SYSTEM)
                .build();
        
        assertEquals(TOPIC, message.getTopic());
        assertEquals(VALUE, message.getValue());
        assertEquals(KEY, message.getKey());
        assertEquals(1, message.getPartition());
        assertEquals(MESSAGE_ID, message.getMessageId());
        assertEquals(CORRELATION_ID, message.getCorrelationId());
        assertEquals(SOURCE_SYSTEM, message.getSourceSystem());
        assertEquals(headers, message.getHeaders());
        assertTrue(message.hasCorrelationId());
    }
    
    @Test
    @DisplayName("Should create copy using toBuilder")
    void shouldCreateCopyUsingToBuilder() {
        KafkaMessage<String> original = KafkaMessage.builder(TOPIC, VALUE)
                .key(KEY)
                .correlationId(CORRELATION_ID)
                .build();
        
        KafkaMessage<String> copy = original.toBuilder()
                .messageId("new-id")
                .build();
        
        assertEquals(original.getTopic(), copy.getTopic());
        assertEquals(original.getValue(), copy.getValue());
        assertEquals(original.getKey(), copy.getKey());
        assertEquals(original.getCorrelationId(), copy.getCorrelationId());
        assertNotEquals(original.getMessageId(), copy.getMessageId());
    }
    
    @Test
    @DisplayName("Should add headers using builder")
    void shouldAddHeadersUsingBuilder() {
        KafkaMessage<String> message = KafkaMessage.builder(TOPIC, VALUE)
                .header("header1", "value1")
                .header("header2", "value2")
                .build();
        
        assertEquals("value1", message.getHeaders().get("header1"));
        assertEquals("value2", message.getHeaders().get("header2"));
        assertEquals(2, message.getHeaders().size());
    }
    
    @Test
    @DisplayName("Should add standard headers")
    void shouldAddStandardHeaders() {
        KafkaMessage<String> message = KafkaMessage.builder(TOPIC, VALUE)
                .correlationId(CORRELATION_ID)
                .sourceSystem(SOURCE_SYSTEM)
                .withStandardHeaders()
                .build();
        
        assertTrue(message.getHeaders().containsKey("message-id"));
        assertTrue(message.getHeaders().containsKey("timestamp"));
        assertEquals(CORRELATION_ID, message.getHeaders().get("correlation-id"));
        assertEquals(SOURCE_SYSTEM, message.getHeaders().get("source-system"));
    }
    
    @Test
    @DisplayName("Should throw exception for null topic")
    void shouldThrowExceptionForNullTopic() {
        assertThrows(NullPointerException.class, () -> 
            KafkaMessage.builder(null, VALUE));
    }
    
    @Test
    @DisplayName("Should throw exception for null value")
    void shouldThrowExceptionForNullValue() {
        assertThrows(NullPointerException.class, () -> 
            KafkaMessage.builder(TOPIC, null));
    }
    
    @Test
    @DisplayName("Should implement equals and hashCode based on messageId")
    void shouldImplementEqualsAndHashCodeBasedOnMessageId() {
        KafkaMessage<String> message1 = KafkaMessage.builder(TOPIC, VALUE)
                .messageId(MESSAGE_ID)
                .build();
        
        KafkaMessage<String> message2 = KafkaMessage.builder(TOPIC, "different-value")
                .messageId(MESSAGE_ID)
                .build();
        
        KafkaMessage<String> message3 = KafkaMessage.builder(TOPIC, VALUE)
                .messageId("different-id")
                .build();
        
        assertEquals(message1, message2);
        assertEquals(message1.hashCode(), message2.hashCode());
        assertNotEquals(message1, message3);
        assertNotEquals(message1.hashCode(), message3.hashCode());
    }
    
    @Test
    @DisplayName("Should provide meaningful toString")
    void shouldProvideMeaningfulToString() {
        KafkaMessage<String> message = KafkaMessage.builder(TOPIC, VALUE)
                .key(KEY)
                .correlationId(CORRELATION_ID)
                .build();
        
        String toString = message.toString();
        assertTrue(toString.contains(message.getMessageId()));
        assertTrue(toString.contains(TOPIC));
        assertTrue(toString.contains(KEY));
        assertTrue(toString.contains(CORRELATION_ID));
    }
}
