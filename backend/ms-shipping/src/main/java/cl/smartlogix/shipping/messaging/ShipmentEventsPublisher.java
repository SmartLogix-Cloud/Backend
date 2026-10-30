package cl.smartlogix.shipping.messaging;

import cl.smartlogix.shipping.messaging.event.ShipmentDeliveredEvent;
import cl.smartlogix.shipping.messaging.event.ShipmentLostEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Productor de eventos del dominio Envio. Publica al exchange de logistica los
 * hitos que cambian la maquina de estados del pedido.
 */
@Component
@Slf4j
public class ShipmentEventsPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ShipmentEventsPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /** Publica que el envio fue entregado. */
    public void publishDelivered(Long orderId, String trackingNumber) {
        ShipmentDeliveredEvent event = new ShipmentDeliveredEvent(
            UUID.randomUUID().toString(), orderId, trackingNumber, OffsetDateTime.now().toString());
        send(RabbitTopologyConfig.RK_SHIPMENT_DELIVERED, event);
    }

    /** Publica que el envio se perdio. */
    public void publishLost(Long orderId, String trackingNumber, String reason) {
        ShipmentLostEvent event = new ShipmentLostEvent(
            UUID.randomUUID().toString(), orderId, trackingNumber, reason, OffsetDateTime.now().toString());
        send(RabbitTopologyConfig.RK_SHIPMENT_LOST, event);
    }

    private void send(String routingKey, Object event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            // Los eventos del dominio Envio viajan por el exchange TOPIC (enrutamiento por patron).
            rabbitTemplate.convertAndSend(RabbitTopologyConfig.EVENTS_EXCHANGE, routingKey, json);
            log.info("Evento publicado [{}]: {}", routingKey, json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar el evento " + routingKey, e);
        }
    }
}
