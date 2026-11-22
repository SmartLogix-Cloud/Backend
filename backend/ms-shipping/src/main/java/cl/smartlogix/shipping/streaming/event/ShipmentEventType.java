package cl.smartlogix.shipping.streaming.event;

/** Tipos de evento de seguimiento de envios publicados en Kafka. */
public enum ShipmentEventType {
    /** Envio creado (manual o automaticamente desde un pedido listo para despacho). */
    SHIPMENT_CREATED,
    /** Se asigno un transportista al envio. */
    CARRIER_ASSIGNED,
    /** Cambio de estado del envio (retirado, en transito, entregado, perdido, ...). */
    SHIPMENT_STATUS_CHANGED
}
