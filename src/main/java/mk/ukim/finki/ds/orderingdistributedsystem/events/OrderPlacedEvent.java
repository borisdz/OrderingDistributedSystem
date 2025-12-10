package mk.ukim.finki.ds.orderingdistributedsystem.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import mk.ukim.finki.ds.orderingdistributedsystem.OrderItem;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderPlacedEvent {
    private String orderId;
    private String customerRegion;
    private List<OrderItem> items;
}
