package mk.ukim.finki.ds.orderingdistributedsystem;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotBlank String customerRegion,
        @NotEmpty @Valid List<OrderItem> items
        ) {}
