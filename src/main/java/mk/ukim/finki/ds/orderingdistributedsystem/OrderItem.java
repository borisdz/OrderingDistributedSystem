package mk.ukim.finki.ds.orderingdistributedsystem;

import jakarta.persistence.Embeddable;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Embeddable
@Data
@NoArgsConstructor
@AllArgsConstructor
public final class OrderItem {

    @NotBlank
    private String productId;

    @Positive
    private int quantity;
}
