package mk.ukim.finki.ds.orderingdistributedsystem.web;

import mk.ukim.finki.ds.orderingdistributedsystem.service.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderStatusControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private OrderService orderService;

    @Test
    void returnsCurrentStatusForKnownOrder() throws Exception {
        when(orderService.findOrderStatus("order-1")).thenReturn(Optional.of("AVAILABLE"));

        mockMvc.perform(get("/api/orders/order-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value("order-1"))
                .andExpect(jsonPath("$.status").value("AVAILABLE"));
    }

    @Test
    void returnsNotFoundForUnknownOrder() throws Exception {
        when(orderService.findOrderStatus("missing")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/orders/missing"))
                .andExpect(status().isNotFound());
    }
}
