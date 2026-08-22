package mk.ukim.finki.ds.orderingdistributedsystem.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import mk.ukim.finki.ds.contracts.events.OrderPlacedEvent;
import mk.ukim.finki.ds.orderingdistributedsystem.CreateOrderRequest;
import mk.ukim.finki.ds.orderingdistributedsystem.IdempotencyRepository;
import mk.ukim.finki.ds.orderingdistributedsystem.KafkaTopicConfig;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderItem;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderRepository;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderServiceEventPublishingTest {

    @Mock
    private OrderRepository orderRepository;
    @Mock
    private IdempotencyRepository idempotencyRepository;
    @Mock
    private KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;
    @Mock
    private SimpMessagingTemplate messagingTemplate;
    @Mock
    private Tracer tracer;

    private SimpleMeterRegistry meterRegistry;
    private OrderService orderService;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        meterRegistry = new SimpleMeterRegistry();
        KafkaTopicConfig kafkaTopicConfig = new KafkaTopicConfig();
        kafkaTopicConfig.setOrderPlacedUs("order-placed.us");
        kafkaTopicConfig.setOrderPlacedEu("order-placed.eu");

        var spanBuilder = mock(io.opentelemetry.api.trace.SpanBuilder.class);
        when(tracer.spanBuilder(anyString())).thenReturn(spanBuilder);
        when(spanBuilder.setAttribute(anyString(), anyString())).thenReturn(spanBuilder);
        var span = mock(io.opentelemetry.api.trace.Span.class);
        when(spanBuilder.startSpan()).thenReturn(span);
        when(span.makeCurrent()).thenReturn(mock(io.opentelemetry.context.Scope.class));
        when(span.setAttribute(anyString(), anyString())).thenReturn(span);

        orderService = new OrderService(orderRepository, idempotencyRepository, kafkaTemplate,
                messagingTemplate, tracer, kafkaTopicConfig, meterRegistry);
    }

    @Test
    void successfulPublishIncrementsSuccessCounter() {
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("order-placed.us", 0), 0, 0, 0, 0, 0);
        SendResult<String, OrderPlacedEvent> sendResult = new SendResult<>(null, metadata);
        when(kafkaTemplate.send(anyString(), anyString(), any(OrderPlacedEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        CreateOrderRequest request = new CreateOrderRequest("cust-1", "us",
                List.of(new OrderItem("P1", 1)));

        orderService.createOrder(request);

        await().untilAsserted(() -> {
            Counter counter = meterRegistry.find("order.event.publish").tag("outcome", "success").counter();
            assertEquals(1.0, counter.count());
        });
    }

    @Test
    void failedPublishIncrementsFailureCounter() {
        CompletableFuture<SendResult<String, OrderPlacedEvent>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("broker unavailable"));
        when(kafkaTemplate.send(anyString(), anyString(), any(OrderPlacedEvent.class)))
                .thenReturn(failedFuture);

        CreateOrderRequest request = new CreateOrderRequest("cust-2", "eu",
                List.of(new OrderItem("P2", 1)));

        orderService.createOrder(request);

        await().untilAsserted(() -> {
            Counter counter = meterRegistry.find("order.event.publish").tag("outcome", "failure").counter();
            assertEquals(1.0, counter.count());
        });
    }
}
