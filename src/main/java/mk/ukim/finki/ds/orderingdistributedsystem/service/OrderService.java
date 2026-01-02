package mk.ukim.finki.ds.orderingdistributedsystem.service;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import lombok.RequiredArgsConstructor;
import mk.ukim.finki.ds.orderingdistributedsystem.CreateOrderRequest;
import mk.ukim.finki.ds.orderingdistributedsystem.Order;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderItem;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderRepository;
import mk.ukim.finki.ds.orderingdistributedsystem.events.OrderPlacedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final Tracer tracer;

    public record OrderStatusUpdate(String orderId, String status) {}

    public void sendStatusUpdate(String orderId, String status) {
        messagingTemplate.convertAndSend("/topic/order/" + orderId,
                new OrderStatusUpdate(orderId, status));
    }

    @Transactional
    public String createOrder(CreateOrderRequest request) {
        Span span = tracer.spanBuilder("order-service.createOrder")
                .setAttribute("customer.id", request.customerId())
                .setAttribute("customer.region", request.customerRegion())
                .startSpan();

        String orderId;
        try (var scope = span.makeCurrent()) {
            orderId = UUID.randomUUID().toString();
            span.setAttribute("order.id", orderId);

            // Persist order
            Order order = Order.builder()
                    .orderId(orderId)
                    .customerId(request.customerId())
                    .customerRegion(request.customerRegion().toLowerCase())
                    .status("PENDING")
                    .items(request.items().stream()
                            .map(i -> new OrderItem(i.getProductId(), i.getQuantity()))
                            .toList())
                    .build();

            orderRepository.save(order);

            // Publish to region-specific Kafka topic
            String topic = "order-placed." + request.customerRegion().toLowerCase();
            OrderPlacedEvent event = new OrderPlacedEvent(
                    orderId,
                    request.customerRegion().toLowerCase(),
                    request.items().stream()
                            .map(i -> new OrderItem(i.getProductId(), i.getQuantity()))
                            .toList());

            kafkaTemplate.send(topic, orderId, event);

            // Initial status update via WebSocket
            sendStatusUpdate(orderId, "ORDER_PLACED");

            span.addEvent("order.created.and.published");
            return orderId;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}