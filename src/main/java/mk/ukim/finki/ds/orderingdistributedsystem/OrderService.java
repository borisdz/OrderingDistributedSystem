package mk.ukim.finki.ds.orderingdistributedsystem;

import lombok.RequiredArgsConstructor;
import mk.ukim.finki.ds.orderingdistributedsystem.events.OrderPlacedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;

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

        return orderId;
    }
}
