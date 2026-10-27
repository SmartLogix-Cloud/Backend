package cl.smartlogix.order.controller;

import cl.smartlogix.order.dto.CreateOrderRequest;
import cl.smartlogix.order.dto.OrderDto;
import cl.smartlogix.order.dto.UpdateOrderState;
import cl.smartlogix.order.model.OrderStatus;
import cl.smartlogix.order.model.OrderType;
import cl.smartlogix.order.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class OrderControllerTest {

    private MockMvc mockMvc;

    private OrderService service;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {

        service = Mockito.mock(OrderService.class);

        OrderController controller =
                new OrderController(service);

        mockMvc = MockMvcBuilders
                .standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();

        objectMapper = new ObjectMapper();
    }

    private OrderDto buildOrderDto() {

        return new OrderDto(
                1L,
                "PED-20250621-ABC123",
                OrderType.ESTANDAR,
                OrderStatus.PENDIENTE_PREPARACION,
                "CLIENTE-1",
                "SHOPIFY",
                new BigDecimal("1000.00"),
                new BigDecimal("190.00"),
                new BigDecimal("1190.00"),
                List.of(),
                OffsetDateTime.now(),
                OffsetDateTime.now()
        );
    }

    @Test
    void createShouldReturnCreated() throws Exception {

        CreateOrderRequest.ItemRequest item =
                new CreateOrderRequest.ItemRequest(
                        1L,
                        "SKU-001",
                        2,
                        new BigDecimal("500")
                );

        CreateOrderRequest request =
                new CreateOrderRequest(
                        OrderType.ESTANDAR,
                        "CLIENTE-1",
                        "SHOPIFY",
                        List.of(item)
                );

        when(service.create(any(CreateOrderRequest.class)))
                .thenReturn(buildOrderDto());

        mockMvc.perform(
                        post("/orders")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request))
                )
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.orderId").value(1));

        verify(service).create(any(CreateOrderRequest.class));
    }

    @Test
    void getByIdShouldReturnOrder() throws Exception {

        when(service.findById(1L))
                .thenReturn(buildOrderDto());

        mockMvc.perform(get("/orders/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(1));

        verify(service).findById(1L);
    }

    @Test
    void getByCodeShouldReturnOrder() throws Exception {

        when(service.findByCode("PED-001"))
                .thenReturn(buildOrderDto());

        mockMvc.perform(get("/orders/code/PED-001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(1));

        verify(service).findByCode("PED-001");
    }

    @Test
    void getByCustomerShouldReturnOrders() throws Exception {

        when(service.findByCustomer("CLIENTE-1"))
                .thenReturn(List.of(buildOrderDto()));

        mockMvc.perform(get("/orders/customer/CLIENTE-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].orderId").value(1));

        verify(service).findByCustomer("CLIENTE-1");
    }

    @Test
    void listShouldReturnAllOrders() throws Exception {

        when(service.findAll())
                .thenReturn(List.of(buildOrderDto()));

        mockMvc.perform(get("/orders"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].orderId").value(1));

        verify(service).findAll();
    }

    @Test
    void listShouldFilterByStatus() throws Exception {

        when(service.findByStatus(OrderStatus.PENDIENTE_PREPARACION))
                .thenReturn(List.of(buildOrderDto()));

        mockMvc.perform(
                        get("/orders")
                                .param("status", "PENDIENTE_PREPARACION")
                )
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].status").value("PENDIENTE_PREPARACION"));

        verify(service).findByStatus(OrderStatus.PENDIENTE_PREPARACION);
    }

    @Test
    void changeStatusShouldReturnUpdatedOrder() throws Exception {

        UpdateOrderState request =
                new UpdateOrderState(
                        OrderStatus.APROBADO,
                        "Pedido aprobado"
                );

        OrderDto updated =
                new OrderDto(
                        1L,
                        "PED-20250621-ABC123",
                        OrderType.ESTANDAR,
                        OrderStatus.APROBADO,
                        "CLIENTE-1",
                        "SHOPIFY",
                        new BigDecimal("1000.00"),
                        new BigDecimal("190.00"),
                        new BigDecimal("1190.00"),
                        List.of(),
                        OffsetDateTime.now(),
                        OffsetDateTime.now()
                );

        when(service.changeStatus(eq(1L), any(UpdateOrderState.class)))
                .thenReturn(updated);

        mockMvc.perform(
                        patch("/orders/1/status")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request))
                )
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APROBADO"));

        verify(service).changeStatus(eq(1L), any(UpdateOrderState.class));
    }
}