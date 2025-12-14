package mk.ukim.finki.ds.orderingdistributedsystem;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import lombok.RequiredArgsConstructor;
import mk.ukim.finki.ds.orderingdistributedsystem.events.OrderPlacedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor  // This will inject all final fields
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final Tracer tracer;  // Injected via constructor — clean and testable

    public record OrderStatusUpdate(String orderId, String status) {
    }

    public void sendStatusUpdate(String orderId, String status) {
        messagingTemplate.convertAndSend("/topic/order/" + orderId,
                new OrderStatusUpdate(orderId, status));
    }

    @Transactional
    public String createOrder(CreateOrderRequest request) {
        // Start parent span for the entire order creation process
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
                    .items(request.items().stream()
                            .map(i -> new OrderItem(i.getProductId(), i.getQuantity()))
                            .toList())
                    .build();

            orderRepository.save(order);

            // Publish to Kafka
            String topic = "order-placed." + request.customerRegion().toLowerCase();
            OrderPlacedEvent event = new OrderPlacedEvent(
                    orderId,
                    request.customerRegion().toLowerCase(),
                    request.items().stream()
                            .map(i -> new OrderItem(i.getProductId(), i.getQuantity()))
                            .toList());

            kafkaTemplate.send(topic, orderId, event);

            // Initial status update
            sendStatusUpdate(orderId, "ORDER_PLACED");

            // Simulate warehouse checks (outside span — acceptable for demo)
            simulateWarehouseResponses(orderId);

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

    private void simulateWarehouseResponses(String orderId) {
        // Capture the full current Context (includes the parent span)
        Context parentContext = Context.current();

        new Thread(() -> {
            try (var scope = parentContext.makeCurrent()) {

                Span checkingUs = tracer.spanBuilder("warehouse.check.us").startSpan();
                try (var usScope = checkingUs.makeCurrent()) {
                    Thread.sleep(2000);
                    sendStatusUpdate(orderId, "CHECKING_US_WAREHOUSE");
                    checkingUs.addEvent("us.check.completed");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    checkingUs.recordException(e);
                    checkingUs.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
                } finally {
                    checkingUs.end();
                }

                Span checkingEu = tracer.spanBuilder("warehouse.check.eu").startSpan();
                try (var euScope = checkingEu.makeCurrent()) {
                    Thread.sleep(1500);
                    sendStatusUpdate(orderId, "CHECKING_EU_WAREHOUSE");
                    checkingEu.addEvent("eu.check.completed");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    checkingEu.recordException(e);
                    checkingEu.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
                } finally {
                    checkingEu.end();
                }

                Span availability = tracer.spanBuilder("warehouse.availability.determined").startSpan();
                try (var availScope = availability.makeCurrent()) {
                    Thread.sleep(2000);
                    sendStatusUpdate(orderId, "AVAILABLE_IN_EU");
                    availability.setAttribute("warehouse.selected", "EU");
                    availability.addEvent("availability.confirmed");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    availability.recordException(e);
                    availability.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
                } finally {
                    availability.end();
                }

                Span reservation = tracer.spanBuilder("warehouse.reservation").startSpan();
                try (var resScope = reservation.makeCurrent()) {
                    Thread.sleep(1000);
                    sendStatusUpdate(orderId, "RESERVED");
                    reservation.addEvent("reservation.completed");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    reservation.recordException(e);
                    reservation.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
                } finally {
                    reservation.end();
                }

            } catch (Exception e) {
                Span.current().recordException(e);
                Span.current().setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
            }
        }).start();
    }
}