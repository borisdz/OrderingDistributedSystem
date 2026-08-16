package mk.ukim.finki.ds.orderingdistributedsystem.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mk.ukim.finki.ds.contracts.events.AvailabilityCheckedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class AvailabilityResponseService {

    private final AvailabilityAggregatorService aggregatorService;

    @KafkaListener(
            topics = "availability-response",
            groupId = "order-service-aggregator",
            containerFactory = "availabilityKafkaListenerContainerFactory"
    )
    public void onAvailabilityChecked(AvailabilityCheckedEvent event){
        log.info("Received AvailabilityCheckedEvent: orderId={}, warehouse={}, available={}",
                event.getOrderId(), event.getWarehouseRegion(), event.isAvailable());
        aggregatorService.processAvailabilityResponse(event);
    }
}
