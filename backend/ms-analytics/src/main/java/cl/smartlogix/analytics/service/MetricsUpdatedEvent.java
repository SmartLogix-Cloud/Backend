package cl.smartlogix.analytics.service;

import cl.smartlogix.analytics.dto.RecentEvent;

/** Evento interno de Spring: las metricas cambiaron por un nuevo evento Kafka. */
public record MetricsUpdatedEvent(RecentEvent cause) {}
