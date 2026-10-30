package cl.smartlogix.order.messaging;

import cl.smartlogix.order.messaging.event.OrderCancelledEvent;
import cl.smartlogix.order.messaging.event.OrderReadyForShipmentEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Productor de eventos del dominio Pedido. Publica al exchange de logistica.
 * Centraliza la serializacion a JSON y el uso de las constantes de topologia.
 */
@Component
@Slf4j
public class OrderEventsPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderEventsPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /** Publica que el pedido esta listo para generar su envio. */
    public void publishReadyForShipment(Long orderId, String orderCode) {
        OrderReadyForShipmentEvent event = new OrderReadyForShipmentEvent(
            UUID.randomUUID().toString(), orderId, orderCode, OffsetDateTime.now().toString());
        send(RabbitTopologyConfig.RK_ORDER_READY, event);
    }

    /** Publica que el pedido fue cancelado. */
    public void publishCancelled(Long orderId, String orderCode) {
        OrderCancelledEvent event = new OrderCancelledEvent(
            UUID.randomUUID().toString(), orderId, orderCode, OffsetDateTime.now().toString());
        send(RabbitTopologyConfig.RK_ORDER_CANCELLED, event);
    }

    private void send(String routingKey, Object event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            // Los comandos del dominio Pedido viajan por el exchange DIRECT (enrutamiento exacto).
            rabbitTemplate.convertAndSend(RabbitTopologyConfig.COMMANDS_EXCHANGE, routingKey, json);
            log.info("Comando publicado [{}]: {}", routingKey, json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar el comando " + routingKey, e);
        }
    }
}
