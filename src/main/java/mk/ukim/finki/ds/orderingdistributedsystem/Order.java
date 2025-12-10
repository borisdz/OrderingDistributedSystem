package mk.ukim.finki.ds.orderingdistributedsystem;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.time.Instant;
import java.util.List;

@Entity
@Table(name = "orders")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {
    @Id
    private String orderId;

    private String customerId;
    private String customerRegion;
    private String status = "PENDING";

    @ElementCollection
    private List<OrderItem> items;

    private Instant createdAt = Instant.now();

}
