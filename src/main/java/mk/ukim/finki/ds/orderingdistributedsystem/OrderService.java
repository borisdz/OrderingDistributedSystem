package mk.ukim.finki.ds.orderingdistributedsystem;

import lombok.RequiredArgsConstructor;
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
    public final SimpMessagingTemplate messagingTemplate;

    public record OrderStatusUpdate(String orderId, String status) {}

    public void sendStatusUpdate(String orderId, String status){
        messagingTemplate.convertAndSend("/topic/order/" + orderId,
                new OrderStatusUpdate(orderId, status));
    }

    @Transactional
    public String createOrder(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        Order order = Order.builder()
                .orderId(orderId)
                .customerId(request.customerId())
                .customerRegion(request.customerRegion())
                .items(request.items().stream()
                        .map(i -> new OrderItem(i.getProductId(), i.getQuantity()))
                        .toList())
                .build();

        orderRepository.save(order);

        String topic = "order-placed." + request.customerRegion().toLowerCase();

        OrderPlacedEvent event = new OrderPlacedEvent(
                orderId,
                request.customerRegion().toLowerCase(),
                request.items().stream()
                        .map(i -> new OrderItem(i.getProductId(), i.getQuantity()))
                        .toList());

        kafkaTemplate.send(topic, orderId, event);

        sendStatusUpdate(orderId, "Order Placed");
        simulateWarehouseResponses(orderId);

        return orderId;
    }

    private void simulateWarehouseResponses(String orderId){
        new Thread(()->{
            try {
                Thread.sleep(2000);
                sendStatusUpdate(orderId, "CHECKING_US_WAREHOUSE");
                Thread.sleep(1500);
                sendStatusUpdate(orderId, "CHECKING_EU_WAREHOUSE");
                Thread.sleep(2000);
                sendStatusUpdate(orderId, "AVAILABLE_IN_EU");
                Thread.sleep(1000);
                sendStatusUpdate(orderId, "RESERVED");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
}
