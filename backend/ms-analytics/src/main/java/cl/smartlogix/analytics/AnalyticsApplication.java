package cl.smartlogix.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * ms-analytics: analitica logistica en tiempo real.
 * Consume los topicos Kafka de pedidos y envios y mantiene metricas operacionales
 * (pedidos por estado, ventas, envios por estado, incidencias) expuestas por REST/SSE.
 */
@SpringBootApplication
public class AnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }
}
