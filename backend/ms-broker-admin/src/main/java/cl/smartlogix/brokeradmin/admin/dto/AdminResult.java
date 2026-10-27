package cl.smartlogix.brokeradmin.admin.dto;

/**
 * Resultado uniforme de una operacion de administracion sobre el broker.
 *
 * @param resource tipo de recurso afectado (queue, exchange, binding)
 * @param name     nombre o identificador del recurso
 * @param action   accion realizada (created, deleted, bound, unbound)
 * @param ok       si la operacion se aplico en el broker
 */
public record AdminResult(
    String resource,
    String name,
    String action,
    boolean ok
) {}
