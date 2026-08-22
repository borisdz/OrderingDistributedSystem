package mk.ukim.finki.ds.orderingdistributedsystem.service;

import mk.ukim.finki.ds.contracts.events.AvailabilityCheckedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mk.ukim.finki.ds.orderingdistributedsystem.AggregationConfig;
import mk.ukim.finki.ds.orderingdistributedsystem.Order;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class AvailabilityAggregatorService {

    private final OrderService orderService;
    private final OrderRepository orderRepository;
    private final AggregationConfig aggregationConfig;

    private final Map<String, AggregationState> stateMap = new ConcurrentHashMap<>();

    public void processAvailabilityResponse(AvailabilityCheckedEvent event) {
        String orderId = Objects.requireNonNull(event.getOrderId(), "orderId must not be null");
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null || isTerminal(order.getStatus())) {
            log.info("Ignoring response for unknown or terminal orderId={}", orderId);
            return;
        }

        int expectedCount = aggregationConfig.getExpectedResponses()
                .getOrDefault(order.getCustomerRegion().toLowerCase(), 1);
        AggregationState state = stateMap.computeIfAbsent(orderId,
                id -> new AggregationState(id, expectedCount, aggregationConfig.getTimeoutSeconds()));
        state.addResponse(event);

        if (state.isComplete()) {
            aggregateAndNotify(orderId, state.getResponses().values().stream().toList(), order);
            stateMap.remove(orderId, state);
        } else {
            orderService.sendStatusUpdate(orderId,
                    String.format("CHECKING_WAREHOUSES (%d/%d)", state.getResponses().size(), expectedCount));
        }
    }

    @Scheduled(fixedDelayString = "${aggregation.cleanup-interval-seconds:10}000")
    public void cleanupTimedOutResponses() {
        Instant now = Instant.now();
        stateMap.forEach((orderId, state) -> {
            if (!state.isTimedOut(now) || !stateMap.remove(orderId, state)) {
                return;
            }
            orderRepository.findById(Objects.requireNonNull(orderId, "orderId must not be null")).ifPresent(order -> {
                if (!isTerminal(order.getStatus())) {
                    order.setStatus("AVAILABILITY_TIMEOUT");
                    orderRepository.save(order);
                    orderService.sendStatusUpdate(orderId, "AVAILABILITY_CHECK_TIMED_OUT");
                }
            });
        });
    }

    private void aggregateAndNotify(String orderId, List<AvailabilityCheckedEvent> responses, Order order){
        AvailabilityCheckedEvent bestOption = responses.stream()
                .filter(response -> response != null && response.isAvailable())
                .min(Comparator.comparingInt(response -> response.getEtaHours()))
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

    private boolean isTerminal(String status) {
        return "AVAILABLE".equals(status)
                || "UNAVAILABLE".equals(status)
                || "AVAILABILITY_TIMEOUT".equals(status);
    }
}
