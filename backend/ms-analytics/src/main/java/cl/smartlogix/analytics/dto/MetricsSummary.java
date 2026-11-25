package cl.smartlogix.analytics.dto;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Foto de las metricas logisticas calculadas en tiempo real desde Kafka.
 *
 * @param totalOrders        pedidos distintos observados
 * @param ordersByStatus     pedidos por estado actual
 * @param ordersByType       pedidos por tipo (STANDARD / EXPRESS)
 * @param activeRevenue      suma de totales de pedidos no cancelados ni rechazados
 * @param totalShipments     envios distintos observados
 * @param shipmentsByStatus  envios por estado actual
 * @param activeIncidents    envios en PERDIDO / RETRASADO / ENTREGA_FALLIDA
 * @param deliveryRate       % de envios entregados sobre el total
 * @param eventsProcessed    eventos distintos aplicados (ambos topicos)
 * @param duplicatesIgnored  eventos descartados por duplicados o fuera de orden
 * @param lastEventAt        instante del ultimo evento aplicado
 * @param generatedAt        instante en que se calculo esta foto
 */
public record MetricsSummary(
    long totalOrders,
    Map<String, Long> ordersByStatus,
    Map<String, Long> ordersByType,
    BigDecimal activeRevenue,
    long totalShipments,
    Map<String, Long> shipmentsByStatus,
    long activeIncidents,
    double deliveryRate,
    long eventsProcessed,
    long duplicatesIgnored,
    String lastEventAt,
    String generatedAt
) {}
