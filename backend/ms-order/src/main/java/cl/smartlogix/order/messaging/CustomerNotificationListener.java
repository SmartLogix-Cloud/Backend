package cl.smartlogix.order.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidor de NOTIFICACIÓN (dominio Notificaciones) — EP2.
 *
 * <p>Escucha TODOS los eventos de envío (patrón topic {@code shipment.#}) y simula
 * el aviso al cliente. Es un consumidor independiente del de dominio
 * ({@link ShipmentEventsListener}): sobre el MISMO evento publicado por ms-shipping,
 * el exchange topic hace fan-out hacia varias colas (dominio, notificación, auditoría).
 * Esta es la razón por la que las notificaciones usan un exchange <b>topic</b> y no
 * <b>direct</b> (ver pregunta P6 de la guía).
 *
 * <p>Usa ACK manual: confirma al notificar; ante error hace NACK sin requeue → DLQ.
 */
@Component
@Slf4j
public class CustomerNotificationListener {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = RabbitTopologyConfig.Q_CUSTOMER_NOTIFY)
    public void onShipmentEvent(String payload, Channel channel,
                                @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
                                @Header(AmqpHeaders.RECEIVED_ROUTING_KEY) String routingKey) throws IOException {
        try {
            JsonNode event = objectMapper.readTree(payload);
            long orderId = event.path("orderId").asLong();
            log.info("[NOTIFICACION] Avisando al cliente del pedido {} por evento '{}'", orderId, routingKey);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("[NOTIFICACION] Fallo al notificar, enviando a DLQ. Payload: {}", payload, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
