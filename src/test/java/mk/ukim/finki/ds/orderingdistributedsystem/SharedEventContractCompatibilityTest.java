package mk.ukim.finki.ds.orderingdistributedsystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import mk.ukim.finki.ds.contracts.events.AvailabilityCheckedEvent;
import mk.ukim.finki.ds.contracts.events.OrderPlacedEvent;
import mk.ukim.finki.ds.contracts.model.OrderItem;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SharedEventContractCompatibilityTest {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void orderingCanReadWarehouseAvailabilityResponseContract() throws Exception {
        AvailabilityCheckedEvent event = new AvailabilityCheckedEvent(
                "availability-event-1", AvailabilityCheckedEvent.CURRENT_VERSION,
                Instant.parse("2026-08-16T00:00:00Z"), "correlation-1", "order-1",
                "warehouse-us-1", "us-1", true, 2);

        AvailabilityCheckedEvent restored = objectMapper.readValue(
                objectMapper.writeValueAsString(event), AvailabilityCheckedEvent.class);

        assertEquals(event, restored);
    }

    @Test
    void orderingPublishesCanonicalOrderPlacedContract() throws Exception {
        OrderPlacedEvent event = new OrderPlacedEvent(
                "order-event-1", OrderPlacedEvent.CURRENT_VERSION,
                Instant.parse("2026-08-16T00:00:00Z"), "correlation-1", "order-1",
                "customer-1", "us", List.of(new OrderItem("product-1", 2)));

        OrderPlacedEvent restored = objectMapper.readValue(
                objectMapper.writeValueAsString(event), OrderPlacedEvent.class);

        assertEquals(event, restored);
    }
}