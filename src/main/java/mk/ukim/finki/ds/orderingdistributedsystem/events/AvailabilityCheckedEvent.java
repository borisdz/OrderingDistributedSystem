package mk.ukim.finki.ds.orderingdistributedsystem.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AvailabilityCheckedEvent {
    private String orderId;
    private String warehouseRegion;
    private boolean available;
    private int etaHours;
}
