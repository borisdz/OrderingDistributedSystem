package mk.ukim.finki.ds.orderingdistributedsystem.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import mk.ukim.finki.ds.orderingdistributedsystem.service.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private OrderService orderService;

    @Test
    void validOrderIsAcceptedAndReturnsOrderId() throws Exception {
        when(orderService.createOrder(any(), nullable(String.class))).thenReturn("order-123");

        String body = """
                {"customerId":"cust-1","customerRegion":"us","items":[{"productId":"P1","quantity":2}]}
                """;

        mockMvc.perform(post("/api/orders")
                        .contentType("application/json")
                        .content(body))
                .andExpect(status().isAccepted());
    }

    @Test
    void missingCustomerIdReturnsBadRequestWithFieldErrors() throws Exception {
        String body = """
                {"customerId":"","customerRegion":"us","items":[{"productId":"P1","quantity":2}]}
                """;

        mockMvc.perform(post("/api/orders")
                        .contentType("application/json")
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors.customerId").exists());
    }

    @Test
    void emptyItemsReturnsBadRequest() throws Exception {
        String body = """
                {"customerId":"cust-1","customerRegion":"us","items":[]}
                """;

        mockMvc.perform(post("/api/orders")
                        .contentType("application/json")
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors.items").exists());
    }

    @Test
    void nonPositiveQuantityReturnsBadRequest() throws Exception {
        String body = """
                {"customerId":"cust-1","customerRegion":"us","items":[{"productId":"P1","quantity":0}]}
                """;

        mockMvc.perform(post("/api/orders")
                        .contentType("application/json")
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors['items[0].quantity']").exists());
    }

    @Test
    void blankProductIdReturnsBadRequest() throws Exception {
        String body = """
                {"customerId":"cust-1","customerRegion":"us","items":[{"productId":"","quantity":1}]}
                """;

        mockMvc.perform(post("/api/orders")
                        .contentType("application/json")
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors['items[0].productId']").exists());
    }
}
