package cl.smartlogix.order.service;

import cl.smartlogix.order.dto.UpdateOrderState;
import cl.smartlogix.order.dto.CreateOrderRequest;
import cl.smartlogix.order.dto.OrderDto;
import cl.smartlogix.order.model.OrderStatus;

import java.util.List;

public interface OrderService {
    OrderDto create(CreateOrderRequest req);
    OrderDto findById(Long id);
    OrderDto findByCode(String code);
    List<OrderDto> findAll();
    List<OrderDto> findByStatus(OrderStatus status);
    List<OrderDto> findByCustomer(String customerId);
    OrderDto changeStatus(Long id, UpdateOrderState req);

    // --- Reacciones a eventos de ms-shipping (consumidas via RabbitMQ) ---

    /** El envio del pedido fue entregado: el pedido pasa a FINALIZADO (idempotente). */
    void onShipmentDelivered(Long orderId);

    /** El envio del pedido se perdio: el pedido pasa a INCIDENCIA (idempotente). */
    void onShipmentLost(Long orderId, String reason);
}
