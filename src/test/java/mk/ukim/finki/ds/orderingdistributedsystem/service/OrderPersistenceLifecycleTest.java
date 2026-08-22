package mk.ukim.finki.ds.orderingdistributedsystem.service;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import mk.ukim.finki.ds.contracts.events.OrderPlacedEvent;
import mk.ukim.finki.ds.orderingdistributedsystem.AggregationConfig;
import mk.ukim.finki.ds.orderingdistributedsystem.CreateOrderRequest;
import mk.ukim.finki.ds.orderingdistributedsystem.IdempotencyRepository;
import mk.ukim.finki.ds.orderingdistributedsystem.KafkaTopicConfig;
import mk.ukim.finki.ds.orderingdistributedsystem.Order;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderItem;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderRepository;
import mk.ukim.finki.ds.contracts.events.AvailabilityCheckedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderPersistenceLifecycleTest {

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

    private OrderService orderService;
    private AvailabilityAggregatorService aggregatorService;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        KafkaTopicConfig kafkaTopicConfig = new KafkaTopicConfig();
        kafkaTopicConfig.setOrderPlacedUs("order-placed.us");
        kafkaTopicConfig.setOrderPlacedEu("order-placed.eu");

        SpanBuilder spanBuilder = mock(SpanBuilder.class);
        when(tracer.spanBuilder(anyString())).thenReturn(spanBuilder);
        when(spanBuilder.setAttribute(anyString(), anyString())).thenReturn(spanBuilder);
        Span span = mock(Span.class);
        when(spanBuilder.startSpan()).thenReturn(span);
        when(span.makeCurrent()).thenReturn(mock(Scope.class));
        when(span.setAttribute(anyString(), anyString())).thenReturn(span);
        when(kafkaTemplate.send(anyString(), anyString(), any(OrderPlacedEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("not used in this test")));

        orderService = new OrderService(orderRepository, idempotencyRepository, kafkaTemplate,
                messagingTemplate, tracer, kafkaTopicConfig);

        AggregationConfig aggregationConfig = new AggregationConfig();
        aggregationConfig.setExpectedResponses(Map.of("us", 1, "eu", 1));
        aggregationConfig.setTimeoutSeconds(30);
        aggregatorService = new AvailabilityAggregatorService(orderService, orderRepository, aggregationConfig);
    }

    @Test
    void newOrderIsPersistedWithPendingStatus() {
        CreateOrderRequest request = new CreateOrderRequest("cust-1", "us",
                List.of(new OrderItem("P1", 2)));

        orderService.createOrder(request);

        ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);
        org.mockito.Mockito.verify(orderRepository).save(captor.capture());
        Order saved = captor.getValue();

        assertEquals("PENDING", saved.getStatus());
        assertEquals("cust-1", saved.getCustomerId());
        assertEquals("us", saved.getCustomerRegion());
    }

    @Test
    void availableResponseMovesOrderToAvailable() {
        Order order = Order.builder()
                .orderId("order-1").customerId("cust-1").customerRegion("us")
                .status("PENDING").items(List.of()).build();
        when(orderRepository.findById("order-1")).thenReturn(Optional.of(order));

        AvailabilityCheckedEvent event = new AvailabilityCheckedEvent();
        event.setOrderId("order-1");
        event.setWarehouseRegion("us-1");
        event.setAvailable(true);
        event.setEtaHours(2);

        aggregatorService.processAvailabilityResponse(event);

        assertEquals("AVAILABLE", order.getStatus());
    }

    @Test
    void allUnavailableResponsesMoveOrderToUnavailable() {
        Order order = Order.builder()
                .orderId("order-2").customerId("cust-2").customerRegion("us")
                .status("PENDING").items(List.of()).build();
        when(orderRepository.findById("order-2")).thenReturn(Optional.of(order));

        AvailabilityCheckedEvent event = new AvailabilityCheckedEvent();
        event.setOrderId("order-2");
        event.setWarehouseRegion("us-1");
        event.setAvailable(false);
        event.setEtaHours(0);

        aggregatorService.processAvailabilityResponse(event);

        assertEquals("UNAVAILABLE", order.getStatus());
    }

    @Test
    void lateResponseIsIgnoredAfterTerminalState() {
        Order order = Order.builder()
                .orderId("order-3").customerId("cust-3").customerRegion("us")
                .status("AVAILABLE").items(List.of()).build();
        when(orderRepository.findById("order-3")).thenReturn(Optional.of(order));

        AvailabilityCheckedEvent lateEvent = new AvailabilityCheckedEvent();
        lateEvent.setOrderId("order-3");
        lateEvent.setWarehouseRegion("us-2");
        lateEvent.setAvailable(false);
        lateEvent.setEtaHours(0);

        aggregatorService.processAvailabilityResponse(lateEvent);

        // Terminal state must not be overwritten by a late response.
        assertEquals("AVAILABLE", order.getStatus());
    }
}
