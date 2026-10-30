package cl.smartlogix.shipping.messaging;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidor de AUDITORÍA (dominio Auditoría) — EP2.
 *
 * <p>Escucha TODOS los eventos del exchange topic (patrón {@code #}) y los registra
 * en un log de auditoría. Junto al consumidor de notificación demuestra el fan-out
 * del exchange <b>topic</b>: un único evento publicado se entrega a varias colas
 * (dominio, notificación y auditoría) según sus patrones de routing key. Por eso los
 * eventos de notificación/auditoría usan topic y no direct (ver P6 de la guía).
 *
 * <p>Usa ACK manual: confirma tras auditar; ante error hace NACK sin requeue → DLQ.
 */
@Component
@Slf4j
public class AuditListener {

    @RabbitListener(queues = RabbitTopologyConfig.Q_AUDIT)
    public void onAnyEvent(String payload, Channel channel,
                           @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
                           @Header(AmqpHeaders.RECEIVED_ROUTING_KEY) String routingKey) throws IOException {
        try {
            log.info("[AUDITORIA] evento '{}' registrado: {}", routingKey, payload);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("[AUDITORIA] Fallo al auditar, enviando a DLQ. Payload: {}", payload, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
