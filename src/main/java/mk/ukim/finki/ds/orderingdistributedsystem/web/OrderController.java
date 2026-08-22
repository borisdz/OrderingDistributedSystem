package mk.ukim.finki.ds.orderingdistributedsystem.web;

import lombok.RequiredArgsConstructor;
import mk.ukim.finki.ds.orderingdistributedsystem.CreateOrderRequest;
import mk.ukim.finki.ds.orderingdistributedsystem.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    public record OrderStatusResponse(String orderId, String status) {}

    @PostMapping
    public ResponseEntity<String> createOrder(
            @RequestHeader(value = "X-Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateOrderRequest request){
        String orderId = orderService.createOrder(request, idempotencyKey);
        return ResponseEntity.accepted().body("Order accepted: " + orderId);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderStatusResponse> getOrderStatus(@PathVariable String orderId) {
        return orderService.findOrderStatus(orderId)
                .map(status -> ResponseEntity.ok(new OrderStatusResponse(orderId, status)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
