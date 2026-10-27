package cl.smartlogix.shipping.service;

import cl.smartlogix.shipping.dto.UpdateShipmentRStatusRequest;
import cl.smartlogix.shipping.dto.AssingCarierRequest;
import cl.smartlogix.shipping.dto.CreateShipmentRequest;
import cl.smartlogix.shipping.dto.ShipmentDto;
import cl.smartlogix.shipping.dto.ShipmentTrackingDto;
import cl.smartlogix.shipping.model.ShipmentState;

import java.util.List;

public interface ShipmentService {
    ShipmentDto create(CreateShipmentRequest req);
    ShipmentDto findById(Long id);
    ShipmentDto findByTracking(String trackingNumber);
    List<ShipmentDto> findByOrder(Long orderId);
    List<ShipmentDto> findAll();
    List<ShipmentDto> findByStatus(ShipmentState status);
    List<ShipmentTrackingDto> history(Long shipmentId);

    ShipmentDto assignCarrier(Long shipmentId, AssingCarierRequest req);
    ShipmentDto changeStatus(Long shipmentId, UpdateShipmentRStatusRequest req);

    // --- Reacciones a eventos de ms-order (consumidas via RabbitMQ) ---

    /** Un pedido quedo listo: crea el envio asociado (idempotente por pedido). */
    void onOrderReadyForShipment(Long orderId, String orderCode);

    /** Un pedido fue cancelado: marca para no despachar los envios aun en preparacion. */
    void onOrderCancelled(Long orderId);
}
