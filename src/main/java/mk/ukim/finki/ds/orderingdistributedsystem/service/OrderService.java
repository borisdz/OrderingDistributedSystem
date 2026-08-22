package mk.ukim.finki.ds.orderingdistributedsystem.service;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mk.ukim.finki.ds.contracts.events.OrderPlacedEvent;
import mk.ukim.finki.ds.orderingdistributedsystem.CreateOrderRequest;
import mk.ukim.finki.ds.orderingdistributedsystem.IdempotencyRecord;
import mk.ukim.finki.ds.orderingdistributedsystem.IdempotencyRepository;
import mk.ukim.finki.ds.orderingdistributedsystem.KafkaTopicConfig;
import mk.ukim.finki.ds.orderingdistributedsystem.Order;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderItem;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final IdempotencyRepository idempotencyRepository;
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final Tracer tracer;
    private final KafkaTopicConfig kafkaTopicConfig;

    public record OrderStatusUpdate(String orderId, String status) {}

    public void sendStatusUpdate(String orderId, String status) {
        messagingTemplate.convertAndSend("/topic/order/" + orderId,
                new OrderStatusUpdate(orderId, status));
    }

    @Transactional
    public String createOrder(CreateOrderRequest request, String idempotencyKey) {
        Span span = tracer.spanBuilder("order-service.createOrder")
                .setAttribute("customer.id", request.customerId())
                .setAttribute("customer.region", request.customerRegion())
                .startSpan();

        String orderId;
        try (var scope = span.makeCurrent()) {
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                String payloadHash = payloadHash(request);
                var existing = idempotencyRepository.findById(idempotencyKey);
                if (existing.isPresent()) {
                    if (!existing.get().getPayloadHash().equals(payloadHash)) {
                        throw new IllegalArgumentException("Idempotency key was used with a different request");
                    }
                    return existing.get().getOrderId();
                }
            }
            orderId = UUID.randomUUID().toString();
            span.setAttribute("order.id", orderId);

            // Persist order
            Order order = Objects.requireNonNull(Order.builder()
                    .orderId(orderId)
                    .customerId(request.customerId())
                    .customerRegion(request.customerRegion().toLowerCase())
                    .status("PENDING")
                    .items(request.items().stream()
                            .map(i -> new OrderItem(i.getProductId(), i.getQuantity()))
                            .toList())
                    .build(), "Order must not be null");

            orderRepository.save(order);

            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                idempotencyRepository.save(Objects.requireNonNull(IdempotencyRecord.builder()
                        .idempotencyKey(idempotencyKey)
                        .orderId(orderId)
                        .payloadHash(payloadHash(request))
                        .build()));
            }

            // Publish to region-specific Kafka topic
            String topic = Objects.requireNonNull(
                    kafkaTopicConfig.orderPlacedTopic(request.customerRegion()),
                    "Kafka topic must not be null for region: " + request.customerRegion());
            String eventKey = Objects.requireNonNull(orderId, "Order ID must not be null");
            OrderPlacedEvent event = new OrderPlacedEvent(
                    UUID.randomUUID().toString(),
                    OrderPlacedEvent.CURRENT_VERSION,
                    Instant.now(),
                    orderId,
                    orderId,
                    request.customerId(),
                    request.customerRegion().toLowerCase(),
                    request.items().stream()
                        .map(i -> new mk.ukim.finki.ds.contracts.model.OrderItem(i.getProductId(), i.getQuantity()))
                            .toList());

            kafkaTemplate.send(topic, eventKey, event);

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

    public String createOrder(CreateOrderRequest request) {
        return createOrder(request, null);
    }

    private String payloadHash(CreateOrderRequest request) {
        String payload = request.customerId() + "|" + request.customerRegion().toLowerCase()
                + "|" + request.items().stream()
                .map(item -> item.getProductId() + ":" + item.getQuantity())
                .reduce((left, right) -> left + "," + right)
                .orElse("");
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(payload.getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }
}