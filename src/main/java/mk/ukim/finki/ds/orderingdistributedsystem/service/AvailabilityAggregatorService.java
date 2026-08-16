package mk.ukim.finki.ds.orderingdistributedsystem.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mk.ukim.finki.ds.orderingdistributedsystem.Order;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderRepository;
import mk.ukim.finki.ds.contracts.events.AvailabilityCheckedEvent;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
@RequiredArgsConstructor
@Slf4j
public class AvailabilityAggregatorService {

    private final OrderService orderService;
    private final OrderRepository orderRepository;

    private final Map<String, List<AvailabilityCheckedEvent>> responseMap = new ConcurrentHashMap<>();

    private static final Map<String, Integer> EXPECTED_RESPONSES = Map.of(
            "us", 2,
            "eu", 2
    );

    public void processAvailabilityResponse(AvailabilityCheckedEvent event){
        String orderId = event.getOrderId();
        log.info("Received availability response: orderId={}, warehouse={}, available={}",
                orderId, event.getWarehouseRegion(), event.isAvailable());

        responseMap.compute(orderId, (key, existingList)->{
            if(existingList == null){
                existingList = new CopyOnWriteArrayList<>();
            }
            existingList.add(event);
            return existingList;
        });

        Order order = orderRepository.findById(orderId).orElse(null);
        if(order == null){
            log.warn("Order not found: {}", orderId);
            return;
        }

        String region = order.getCustomerRegion();
        int expectedCount = EXPECTED_RESPONSES.getOrDefault(region,1);
        List<AvailabilityCheckedEvent> responses = responseMap.get(orderId);

        if(responses.size()>=expectedCount){
            aggregateAndNotify(orderId, responses, order);
            responseMap.remove(orderId);
        }else{
            orderService.sendStatusUpdate(orderId,
                    String.format("CHECKING_WAREHOUSES (%d/%d)", responses.size(), expectedCount));
        }
    }

    private void aggregateAndNotify(String orderId, List<AvailabilityCheckedEvent> responses, Order order){
        AvailabilityCheckedEvent bestOption = responses.stream()
                .filter(AvailabilityCheckedEvent::isAvailable)
                .min((a,b)->Integer.compare(a.getEtaHours(), b.getEtaHours()))
                .orElse(null);

        if(bestOption != null){
            log.info("Order {} available at warehouse {} with ETA {} hours",
                    orderId, bestOption.getWarehouseRegion(), bestOption.getEtaHours());

            order.setStatus("AVAILABLE");
            orderRepository.save(order);

            orderService.sendStatusUpdate(orderId,
                    String.format("AVAILABLE_IN_%s (ETA: %dh)",
                            bestOption.getWarehouseRegion().toUpperCase(),
                            bestOption.getEtaHours()));
        } else {
            log.info("Order {} not available in any warehouse", orderId);

            order.setStatus("UNAVAILABLE");
            orderRepository.save(order);

            orderService.sendStatusUpdate(orderId, "UNAVAILABLE_IN_ALL_WAREHOUSES");
        }
    }
}
