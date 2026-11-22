package cl.smartlogix.analytics.dto;

/**
 * Evento recien aplicado, para el feed en tiempo real.
 *
 * @param topic      topico de origen
 * @param partition  particion de origen
 * @param offset     offset dentro de la particion
 * @param eventType  tipo de evento
 * @param entityId   id del pedido o envio
 * @param status     estado resultante
 * @param detail     texto descriptivo
 * @param occurredAt instante del hecho
 */
public record RecentEvent(
    String topic,
    int partition,
    long offset,
    String eventType,
    Long entityId,
    String status,
    String detail,
    String occurredAt
) {}
