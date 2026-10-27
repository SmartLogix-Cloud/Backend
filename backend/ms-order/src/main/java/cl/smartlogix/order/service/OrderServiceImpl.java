package cl.smartlogix.order.service;

import cl.smartlogix.order.dto.UpdateOrderState;
import cl.smartlogix.order.dto.CreateOrderRequest;
import cl.smartlogix.order.dto.OrderDto;
import cl.smartlogix.order.factory.OrderFactory;
import cl.smartlogix.order.factory.OrderFactoryProvider;
import cl.smartlogix.order.messaging.OrderEventsPublisher;
import cl.smartlogix.order.model.*;
import cl.smartlogix.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class OrderServiceImpl implements OrderService {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    // Máquina de estados del pedido (no permite saltos arbitrarios).
    // FINALIZADO e INCIDENCIA se alcanzan desde EN_PREPARACION por eventos del envío.
    private static final Map<OrderStatus, Set<OrderStatus>> TRANSITIONS = Map.of(
        OrderStatus.PENDIENTE_PREPARACION, Set.of(OrderStatus.APROBADO, OrderStatus.RECHAZADO, OrderStatus.CANCELADO),
        OrderStatus.APROBADO,              Set.of(OrderStatus.EN_PREPARACION, OrderStatus.CANCELADO),
        OrderStatus.EN_PREPARACION,        Set.of(OrderStatus.FINALIZADO, OrderStatus.INCIDENCIA, OrderStatus.CANCELADO),
        OrderStatus.INCIDENCIA,            Set.of(OrderStatus.CANCELADO),
        OrderStatus.RECHAZADO,             Set.of(),
        OrderStatus.CANCELADO,             Set.of(),
        OrderStatus.FINALIZADO,            Set.of()
    );

    private final OrderRepository repository;
    private final OrderFactoryProvider factoryProvider;
    private final OrderEventsPublisher eventsPublisher;

    @Override
    public OrderDto create(CreateOrderRequest req) {
        if (req.items() == null || req.items().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "El pedido debe tener al menos un ítem");
        }

        // Factory Method: el creador propio del tipo arma el agregado del pedido.
        OrderFactory factory = factoryProvider.forType(req.type());
        Order order = factory.build(req, generateCode());

        return OrderDto.from(repository.save(order));
    }

    @Override
    @Transactional(readOnly = true)
    public OrderDto findById(Long id) {
        return OrderDto.from(find(id));
    }

    @Override
    @Transactional(readOnly = true)
    public OrderDto findByCode(String code) {
        Order o = repository.findByCode(code)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Pedido no encontrado: " + code));
        return OrderDto.from(o);
    }

    @Override
    @Transactional(readOnly = true)
    public List<OrderDto> findAll() {
        return repository.findAll().stream().map(OrderDto::from).toList();
    }

    @Override
    @Transactional(readOnly = true)
    public List<OrderDto> findByStatus(OrderStatus status) {
        return repository.findByStatus(status).stream().map(OrderDto::from).toList();
    }

    @Override
    @Transactional(readOnly = true)
    public List<OrderDto> findByCustomer(String customerId) {
        return repository.findByCustomerId(customerId).stream().map(OrderDto::from).toList();
    }

    @Override
    public OrderDto changeStatus(Long id, UpdateOrderState req) {
        Order order = find(id);
        OrderStatus current = order.getStatus();
        OrderStatus next = req.status();

        if (!TRANSITIONS.getOrDefault(current, Set.of()).contains(next)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                "Transición no permitida: " + current + " → " + next);
        }

        applyTransition(order, next, req.reason());
        Order saved = repository.save(order);

        // Publicación de eventos de dominio hacia ms-shipping.
        if (next == OrderStatus.EN_PREPARACION) {
            eventsPublisher.publishReadyForShipment(saved.getId(), saved.getCode());
        } else if (next == OrderStatus.CANCELADO) {
            eventsPublisher.publishCancelled(saved.getId(), saved.getCode());
        }

        return OrderDto.from(saved);
    }

    @Override
    public void onShipmentDelivered(Long orderId) {
        Order order = find(orderId);
        if (order.getStatus() == OrderStatus.FINALIZADO) {
            return; // idempotente
        }
        if (order.getStatus() != OrderStatus.EN_PREPARACION) {
            log.warn("shipment.delivered ignorado: pedido {} en estado {} (se esperaba EN_PREPARACION)",
                orderId, order.getStatus());
            return;
        }
        applyTransition(order, OrderStatus.FINALIZADO, "Envío entregado (evento shipment.delivered)");
        repository.save(order);
    }

    @Override
    public void onShipmentLost(Long orderId, String reason) {
        Order order = find(orderId);
        if (order.getStatus() == OrderStatus.INCIDENCIA) {
            return; // idempotente
        }
        if (order.getStatus() != OrderStatus.EN_PREPARACION) {
            log.warn("shipment.lost ignorado: pedido {} en estado {} (se esperaba EN_PREPARACION)",
                orderId, order.getStatus());
            return;
        }
        applyTransition(order, OrderStatus.INCIDENCIA,
            "Envío perdido (evento shipment.lost): " + (reason != null ? reason : "sin detalle"));
        repository.save(order);
    }

    private void applyTransition(Order order, OrderStatus next, String reason) {
        OrderStatus current = order.getStatus();
        order.setStatus(next);
        order.addHistory(OrderHistory.builder()
            .previousStatus(current)
            .newStatus(next)
            .reason(reason)
            .build());
    }

    private Order find(Long id) {
        return repository.findById(id)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Pedido no encontrado: " + id));
    }

    private String generateCode() {
        String date = LocalDate.now().format(DATE_FORMAT);
        String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        return "PED-" + date + "-" + suffix;
    }
}
