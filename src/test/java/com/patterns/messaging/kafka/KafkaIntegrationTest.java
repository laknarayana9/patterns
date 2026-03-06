package com.patterns.messaging.kafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test example showing how to test Kafka components with mocks.
 * This demonstrates the mockable design pattern for easy testing.
 */
class KafkaIntegrationTest {
    
    private MockKafkaProducerService<String> producer;
    private MockKafkaConsumerService<String> consumer;
    private KafkaTemplate<String> kafkaTemplate;
    
    private static final String REQUEST_TOPIC = "user-requests";
    private static final String REPLY_TOPIC = "user-responses";
    private static final String USER_SERVICE_TOPIC = "user-events";
    
    @BeforeEach
    void setUp() {
        producer = new MockKafkaProducerService<>();
        consumer = new MockKafkaConsumerService<>();
        kafkaTemplate = new KafkaTemplate<>(producer, consumer);
    }
    
    @AfterEach
    void tearDown() {
        if (kafkaTemplate != null) {
            kafkaTemplate.close();
        }
    }
    
    @Test
    @DisplayName("Should handle user registration workflow")
    void shouldHandleUserRegistrationWorkflow() throws Exception {
        // Simulate user registration request-response pattern
        String userId = "user-123";
        String userData = "John Doe";
        String correlationId = "reg-" + System.currentTimeMillis();
        
        // Setup consumer to handle registration requests
        CountDownLatch registrationLatch = new CountDownLatch(1);
        AtomicInteger processedRegistrations = new AtomicInteger(0);
        
        kafkaTemplate.subscribe(REQUEST_TOPIC, 
            message -> {
                // Process registration
                String requestUserId = message.getKey();
                String requestUserData = message.getValue();
                
                assertEquals(userId, requestUserId);
                assertEquals(userData, requestUserData);
                
                processedRegistrations.incrementAndGet();
                
                // Send confirmation reply
                KafkaMessage<String> reply = KafkaMessage.builder(REPLY_TOPIC, "User registered: " + requestUserId)
                        .correlationId(message.getCorrelationId())
                        .sourceSystem("user-service")
                        .build();
                
                kafkaTemplate.send(reply);
                registrationLatch.countDown();
            }
        );
        
        kafkaTemplate.startConsumer();
        
        // Send registration request
        CompletableFuture<String> registrationResponse = kafkaTemplate.sendAndReceive(
            REQUEST_TOPIC, REPLY_TOPIC, userData, correlationId, 5000
        );
        
        // Wait for processing
        assertTrue(registrationLatch.await(5, TimeUnit.SECONDS));
        
        // Verify response
        String response = registrationResponse.get(5, TimeUnit.SECONDS);
        assertEquals("User registered: " + userId, response);
        assertEquals(1, processedRegistrations.get());
        
        // Verify metrics
        KafkaTemplate.KafkaMetrics metrics = kafkaTemplate.getMetrics();
        assertTrue(metrics.getProducerMetrics().getMessagesSent() >= 2); // Request + Reply
        assertTrue(metrics.getConsumerMetrics().getMessagesConsumed() >= 1); // Request processed
    }
    
    @Test
    @DisplayName("Should handle event publishing and subscription")
    void shouldHandleEventPublishingAndSubscription() throws InterruptedException {
        // Setup multiple subscribers for user events
        CountDownLatch emailServiceLatch = new CountDownLatch(1);
        CountDownLatch analyticsServiceLatch = new CountDownLatch(1);
        CountDownLatch auditServiceLatch = new CountDownLatch(1);
        
        AtomicInteger emailEvents = new AtomicInteger(0);
        AtomicInteger analyticsEvents = new AtomicInteger(0);
        AtomicInteger auditEvents = new AtomicInteger(0);
        
        // Email service subscriber
        kafkaTemplate.subscribe(List.of(USER_SERVICE_TOPIC), 
            message -> {
                if ("user.created".equals(message.getHeaders().get("event-type"))) {
                    emailEvents.incrementAndGet();
                    emailServiceLatch.countDown();
                }
            },
            message -> "user.created".equals(message.getHeaders().get("event-type"))
        );
        
        // Analytics service subscriber
        kafkaTemplate.subscribe(USER_SERVICE_TOPIC, 
            message -> {
                analyticsEvents.incrementAndGet();
                analyticsServiceLatch.countDown();
            }
        );
        
        // Audit service subscriber (receives all events)
        kafkaTemplate.subscribe(USER_SERVICE_TOPIC, 
            message -> {
                auditEvents.incrementAndGet();
                auditServiceLatch.countDown();
            }
        );
        
        kafkaTemplate.startConsumer();
        
        // Publish user created event
        KafkaMessage<String> userEvent = KafkaMessage.builder(USER_SERVICE_TOPIC, "User created event")
                .key("user-456")
                .header("event-type", "user.created")
                .header("source", "registration-service")
                .sourceSystem("user-service")
                .build();
        
        kafkaTemplate.send(userEvent);
        
        // Wait for all services to process the event
        assertTrue(emailServiceLatch.await(5, TimeUnit.SECONDS));
        assertTrue(analyticsServiceLatch.await(5, TimeUnit.SECONDS));
        assertTrue(auditServiceLatch.await(5, TimeUnit.SECONDS));
        
        // Verify all services received the event
        assertEquals(1, emailEvents.get());
        assertEquals(1, analyticsEvents.get());
        assertEquals(1, auditEvents.get());
    }
    
    @Test
    @DisplayName("Should handle error scenarios gracefully")
    void shouldHandleErrorScenariosGracefully() throws InterruptedException {
        CountDownLatch healthyLatch = new CountDownLatch(2);
        CountDownLatch errorLatch = new CountDownLatch(1);
        
        AtomicInteger processedMessages = new AtomicInteger(0);
        AtomicInteger failedMessages = new AtomicInteger(0);
        
        kafkaTemplate.subscribe(USER_SERVICE_TOPIC, 
            message -> {
                if (message.getValue().contains("error")) {
                    failedMessages.incrementAndGet();
                    errorLatch.countDown();
                    throw new RuntimeException("Simulated processing error");
                } else {
                    processedMessages.incrementAndGet();
                    healthyLatch.countDown();
                }
            }
        );
        
        kafkaTemplate.startConsumer();
        
        // Send mix of healthy and error messages
        kafkaTemplate.send(USER_SERVICE_TOPIC, "healthy-message-1");
        kafkaTemplate.send(USER_SERVICE_TOPIC, "error-message");
        kafkaTemplate.send(USER_SERVICE_TOPIC, "healthy-message-2");
        
        // Wait for processing
        assertTrue(healthyLatch.await(5, TimeUnit.SECONDS));
        assertTrue(errorLatch.await(5, TimeUnit.SECONDS));
        
        // Verify metrics show both successes and failures
        KafkaTemplate.KafkaMetrics metrics = kafkaTemplate.getMetrics();
        assertEquals(2, metrics.getConsumerMetrics().getMessagesConsumed());
        assertEquals(1, metrics.getConsumerMetrics().getMessagesFailed());
        assertTrue(metrics.isHealthy()); // Still healthy because successes > failures
    }
    
    @Test
    @DisplayName("Should handle high message volume")
    void shouldHandleHighMessageVolume() throws Exception {
        int messageCount = 1000;
        CountDownLatch allMessagesProcessed = new CountDownLatch(messageCount);
        AtomicInteger processedCount = new AtomicInteger(0);
        
        kafkaTemplate.subscribe(USER_SERVICE_TOPIC, 
            message -> {
                processedCount.incrementAndGet();
                allMessagesProcessed.countDown();
            }
        );
        
        kafkaTemplate.startConsumer();
        
        // Send many messages
        for (int i = 0; i < messageCount; i++) {
            kafkaTemplate.send(USER_SERVICE_TOPIC, "message-" + i);
        }
        
        // Wait for all messages to be processed
        assertTrue(allMessagesProcessed.await(10, TimeUnit.SECONDS));
        assertEquals(messageCount, processedCount.get());
        
        // Verify metrics
        KafkaTemplate.KafkaMetrics metrics = kafkaTemplate.getMetrics();
        assertEquals(messageCount, metrics.getProducerMetrics().getMessagesSent());
        assertEquals(messageCount, metrics.getConsumerMetrics().getMessagesConsumed());
        assertTrue(metrics.getProducerMetrics().getAverageSendTime() >= 0);
        assertTrue(metrics.getConsumerMetrics().getAverageProcessingTime() >= 0);
    }
    
    @Test
    @DisplayName("Should demonstrate pause and resume functionality")
    void shouldDemonstratePauseAndResumeFunctionality() throws InterruptedException {
        CountDownLatch initialLatch = new CountDownLatch(2);
        CountDownLatch resumeLatch = new CountDownLatch(1);
        
        AtomicInteger initialProcessed = new AtomicInteger(0);
        AtomicInteger resumedProcessed = new AtomicInteger(0);
        
        kafkaTemplate.subscribe(USER_SERVICE_TOPIC, 
            message -> {
                if (initialProcessed.get() < 2) {
                    initialProcessed.incrementAndGet();
                    initialLatch.countDown();
                } else {
                    resumedProcessed.incrementAndGet();
                    resumeLatch.countDown();
                }
            }
        );
        
        kafkaTemplate.startConsumer();
        
        // Send initial messages
        kafkaTemplate.send(USER_SERVICE_TOPIC, "message-1");
        kafkaTemplate.send(USER_SERVICE_TOPIC, "message-2");
        
        // Wait for initial processing
        assertTrue(initialLatch.await(5, TimeUnit.SECONDS));
        assertEquals(2, initialProcessed.get());
        
        // Pause consumer
        kafkaTemplate.stopConsumer();
        
        // Send message while paused (should not be processed)
        kafkaTemplate.send(USER_SERVICE_TOPIC, "paused-message");
        
        Thread.sleep(200); // Give time for potential processing
        
        assertEquals(2, initialProcessed.get()); // Still only 2 processed
        
        // Resume consumer
        kafkaTemplate.startConsumer();
        
        // Send message after resume
        kafkaTemplate.send(USER_SERVICE_TOPIC, "resumed-message");
        
        // Wait for resumed processing
        assertTrue(resumeLatch.await(5, TimeUnit.SECONDS));
        assertEquals(3, initialProcessed.get() + resumedProcessed.get());
    }
}
