package cl.smartlogix.order.streaming.event;

/** Tipos de evento del ciclo de vida del pedido publicados en Kafka. */
public enum OrderEventType {
    /** Pedido registrado (estado inicial). */
    ORDER_CREATED,
    /** Transicion en la maquina de estados (manual o provocada por eventos de envio). */
    ORDER_STATUS_CHANGED
}
