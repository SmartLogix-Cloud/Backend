package cl.smartlogix.order.service;

import cl.smartlogix.order.dto.CreateOrderRequest;
import cl.smartlogix.order.dto.OrderDto;
import cl.smartlogix.order.dto.UpdateOrderState;
import cl.smartlogix.order.factory.ExpressOrderFactory;
import cl.smartlogix.order.factory.OrderFactoryProvider;
import cl.smartlogix.order.factory.StandardOrderFactory;
import cl.smartlogix.order.messaging.OrderEventsPublisher;
import cl.smartlogix.order.model.Order;
import cl.smartlogix.order.model.OrderStatus;
import cl.smartlogix.order.model.OrderType;
import cl.smartlogix.order.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceImplTest {

    @Mock
    private OrderRepository repository;

    @Mock
    private OrderEventsPublisher eventsPublisher;

    private OrderServiceImpl service;

    private CreateOrderRequest request;

    @BeforeEach
    void setUp() {

        // Provider real con los dos creadores concretos (Factory Method).
        OrderFactoryProvider factoryProvider = new OrderFactoryProvider(
                List.of(new StandardOrderFactory(), new ExpressOrderFactory()));
        service = new OrderServiceImpl(repository, factoryProvider, eventsPublisher);

        CreateOrderRequest.ItemRequest item =
                new CreateOrderRequest.ItemRequest(
                        1L,
                        "SKU-001",
                        2,
                        new BigDecimal("1000")
                );

        request = new CreateOrderRequest(
                OrderType.ESTANDAR,
                "CLIENTE-1",
                "SHOPIFY",
                List.of(item)
        );
    }

    @Test
    void createShouldCreateOrderSuccessfully() {

        when(repository.save(any(Order.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        OrderDto dto = service.create(request);

        assertNotNull(dto);
        assertEquals(OrderStatus.PENDIENTE_PREPARACION, dto.status());

        verify(repository).save(any(Order.class));
    }

    @Test
    void createShouldCalculateTotalsCorrectly() {

        ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);

        when(repository.save(any(Order.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.create(request);

        verify(repository).save(captor.capture());

        Order saved = captor.getValue();

        assertEquals(
                new BigDecimal("2000.00"),
                saved.getSubtotal()
        );

        assertEquals(
                new BigDecimal("380.00"),
                saved.getTax()
        );

        assertEquals(
                new BigDecimal("2380.00"),
                saved.getTotal()
        );
    }

    @Test
    void createExpressShouldApplySurcharge() {

        ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);

        when(repository.save(any(Order.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        CreateOrderRequest expressRequest = new CreateOrderRequest(
                OrderType.EXPRESS,
                "CLIENTE-1",
                "SHOPIFY",
                List.of(new CreateOrderRequest.ItemRequest(
                        1L, "SKU-001", 2, new BigDecimal("1000")))
        );

        service.create(expressRequest);

        verify(repository).save(captor.capture());
        Order saved = captor.getValue();

        // subtotal 2000 + recargo 5% (100) = base 2100 -> IVA 399 -> total 2499
        assertEquals(OrderType.EXPRESS, saved.getType());
        assertEquals(new BigDecimal("2000.00"), saved.getSubtotal());
        assertEquals(new BigDecimal("399.00"), saved.getTax());
        assertEquals(new BigDecimal("2499.00"), saved.getTotal());
    }

    @Test
    void createShouldGenerateHistory() {

        ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);

        when(repository.save(any(Order.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.create(request);

        verify(repository).save(captor.capture());

        Order saved = captor.getValue();

        assertEquals(1, saved.getHistory().size());
        assertEquals(
                OrderStatus.PENDIENTE_PREPARACION,
                saved.getHistory().get(0).getNewStatus()
        );
    }

    @Test
    void createShouldThrowWhenItemsAreEmpty() {

        CreateOrderRequest badRequest =
                new CreateOrderRequest(
                        OrderType.ESTANDAR,
                        "CLIENTE-1",
                        null,
                        List.of()
                );

        assertThrows(
                ResponseStatusException.class,
                () -> service.create(badRequest)
        );
    }

    @Test
    void findByIdShouldReturnOrder() {

        Order order = Order.builder()
                .id(1L)
                .code("PED-001")
                .status(OrderStatus.PENDIENTE_PREPARACION)
                .type(OrderType.ESTANDAR)
                .customerId("CLIENTE")
                .build();

        when(repository.findById(1L))
                .thenReturn(Optional.of(order));

        OrderDto dto = service.findById(1L);

        assertEquals("PED-001", dto.code());
    }

    @Test
    void findByIdShouldThrowWhenNotFound() {

        when(repository.findById(1L))
                .thenReturn(Optional.empty());

        assertThrows(
                ResponseStatusException.class,
                () -> service.findById(1L)
        );
    }

    @Test
    void findByCodeShouldReturnOrder() {

        Order order = Order.builder()
                .id(1L)
                .code("PED-001")
                .status(OrderStatus.PENDIENTE_PREPARACION)
                .type(OrderType.ESTANDAR)
                .customerId("CLIENTE")
                .build();

        when(repository.findByCode("PED-001"))
                .thenReturn(Optional.of(order));

        OrderDto dto = service.findByCode("PED-001");

        assertEquals("PED-001", dto.code());
    }

    @Test
    void findByCodeShouldThrowWhenNotFound() {

        when(repository.findByCode("PED-001"))
                .thenReturn(Optional.empty());

        assertThrows(
                ResponseStatusException.class,
                () -> service.findByCode("PED-001")
        );
    }

    @Test
    void findAllShouldReturnOrders() {

        Order order = Order.builder()
                .id(1L)
                .code("PED-001")
                .status(OrderStatus.PENDIENTE_PREPARACION)
                .type(OrderType.ESTANDAR)
                .customerId("CLIENTE")
                .build();

        when(repository.findAll())
                .thenReturn(List.of(order));

        List<OrderDto> result = service.findAll();

        assertEquals(1, result.size());
    }

    @Test
    void findByStatusShouldReturnOrders() {

        Order order = Order.builder()
                .id(1L)
                .code("PED-001")
                .status(OrderStatus.PENDIENTE_PREPARACION)
                .type(OrderType.ESTANDAR)
                .customerId("CLIENTE")
                .build();

        when(repository.findByStatus(OrderStatus.PENDIENTE_PREPARACION))
                .thenReturn(List.of(order));

        List<OrderDto> result =
                service.findByStatus(OrderStatus.PENDIENTE_PREPARACION);

        assertEquals(1, result.size());
    }

    @Test
    void findByCustomerShouldReturnOrders() {

        Order order = Order.builder()
                .id(1L)
                .code("PED-001")
                .status(OrderStatus.PENDIENTE_PREPARACION)
                .type(OrderType.ESTANDAR)
                .customerId("CLIENTE")
                .build();

        when(repository.findByCustomerId("CLIENTE"))
                .thenReturn(List.of(order));

        List<OrderDto> result =
                service.findByCustomer("CLIENTE");

        assertEquals(1, result.size());
    }

    @Test
    void changeStatusShouldUpdateStatus() {

        Order order = Order.builder()
                .id(1L)
                .code("PED-001")
                .status(OrderStatus.PENDIENTE_PREPARACION)
                .type(OrderType.ESTANDAR)
                .customerId("CLIENTE")
                .build();

        when(repository.findById(1L))
                .thenReturn(Optional.of(order));

        when(repository.save(any(Order.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        UpdateOrderState request =
                new UpdateOrderState(
                        OrderStatus.APROBADO,
                        "Aprobado"
                );

        OrderDto dto =
                service.changeStatus(1L, request);

        assertEquals(
                OrderStatus.APROBADO,
                dto.status()
        );
    }

    @Test
    void changeStatusShouldGenerateHistory() {

        Order order = Order.builder()
                .id(1L)
                .code("PED-001")
                .status(OrderStatus.PENDIENTE_PREPARACION)
                .type(OrderType.ESTANDAR)
                .customerId("CLIENTE")
                .build();

        when(repository.findById(1L))
                .thenReturn(Optional.of(order));

        when(repository.save(any(Order.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.changeStatus(
                1L,
                new UpdateOrderState(
                        OrderStatus.APROBADO,
                        "ok"
                )
        );

        assertEquals(1, order.getHistory().size());
    }

    @Test
    void changeStatusShouldThrowForInvalidTransition() {

        Order order = Order.builder()
                .id(1L)
                .code("PED-001")
                .status(OrderStatus.PENDIENTE_PREPARACION)
                .type(OrderType.ESTANDAR)
                .customerId("CLIENTE")
                .build();

        when(repository.findById(1L))
                .thenReturn(Optional.of(order));

        UpdateOrderState request =
                new UpdateOrderState(
                        OrderStatus.FINALIZADO,
                        "salto invalido"
                );

        assertThrows(
                ResponseStatusException.class,
                () -> service.changeStatus(1L, request)
        );
    }

    @Test
    void changeStatusShouldThrowWhenOrderNotFound() {

        when(repository.findById(1L))
                .thenReturn(Optional.empty());

        assertThrows(
                ResponseStatusException.class,
                () -> service.changeStatus(
                        1L,
                        new UpdateOrderState(
                                OrderStatus.APROBADO,
                                "ok"
                        )
                )
        );
    }
}