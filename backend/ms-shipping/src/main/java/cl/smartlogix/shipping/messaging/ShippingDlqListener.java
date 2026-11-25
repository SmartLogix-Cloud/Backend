package cl.smartlogix.shipping.messaging;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidores de las Dead-Letter Queues del dominio Envío (EP2, P8).
 *
 * <p>Cada cola principal tiene su DLQ y aquí se registra en logs todo mensaje que
 * no pudo entregarse (error no recuperable, NACK sin requeue, o caducidad por TTL /
 * exceso de longitud). Se confirma con ACK: el mensaje ya cumplió su ciclo y queda
 * registrado para inspección operativa.
 */
@Component
@Slf4j
public class ShippingDlqListener {

    @RabbitListener(queues = RabbitTopologyConfig.DLQ_ORDER_READY)
    public void onOrderReadyDlq(String payload, Channel channel,
                                @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        log.error("[DLQ] {} mensaje no entregado: {}", RabbitTopologyConfig.DLQ_ORDER_READY, payload);
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = RabbitTopologyConfig.DLQ_ORDER_CANCELLED)
    public void onOrderCancelledDlq(String payload, Channel channel,
                                    @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        log.error("[DLQ] {} mensaje no entregado: {}", RabbitTopologyConfig.DLQ_ORDER_CANCELLED, payload);
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = RabbitTopologyConfig.DLQ_AUDIT)
    public void onAuditDlq(String payload, Channel channel,
                           @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        log.error("[DLQ] {} mensaje no entregado: {}", RabbitTopologyConfig.DLQ_AUDIT, payload);
        channel.basicAck(deliveryTag, false);
    }
}
