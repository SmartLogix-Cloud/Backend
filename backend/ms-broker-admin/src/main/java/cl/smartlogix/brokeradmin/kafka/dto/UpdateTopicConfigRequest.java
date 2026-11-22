package cl.smartlogix.brokeradmin.kafka.dto;

import java.util.List;
import java.util.Map;

/**
 * Modificacion incremental de la configuracion de un topico.
 *
 * @param set    configuraciones a establecer (p.ej. {"retention.ms": "86400000"})
 * @param delete configuraciones a eliminar (vuelven al valor por defecto del broker)
 */
public record UpdateTopicConfigRequest(
    Map<String, String> set,
    List<String> delete
) {}
