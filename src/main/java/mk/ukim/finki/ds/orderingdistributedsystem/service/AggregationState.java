package mk.ukim.finki.ds.orderingdistributedsystem.service;

import lombok.Getter;
import mk.ukim.finki.ds.contracts.events.AvailabilityCheckedEvent;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class AggregationState {
    private final String orderId;
    private final int expectedResponses;
    private final long timeoutSeconds;
    private final Instant startedAt;
    private final Map<String, AvailabilityCheckedEvent> responses = new ConcurrentHashMap<>();

    public AggregationState(String orderId, int expectedResponses, long timeoutSeconds) {
        this.orderId = orderId;
        this.expectedResponses = expectedResponses;
        this.timeoutSeconds = timeoutSeconds;
        this.startedAt = Instant.now();
    }

    public void addResponse(AvailabilityCheckedEvent event) {
        responses.put(event.getWarehouseRegion(), event);
    }

    public boolean isComplete() {
        return responses.size() >= expectedResponses;
    }

    public boolean isTimedOut(Instant now) {
        return !isComplete() && !now.isBefore(startedAt.plusSeconds(timeoutSeconds));
    }
}