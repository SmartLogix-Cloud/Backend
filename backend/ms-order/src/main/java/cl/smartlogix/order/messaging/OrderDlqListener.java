package cl.smartlogix.order.messaging;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidores de las Dead-Letter Queues del dominio Pedido (EP2, P8).
 *
 * <p>Cada cola principal tiene su DLQ y aquí se registra en logs todo mensaje que
 * terminó sin poder entregarse (error no recuperable, NACK sin requeue, o caducidad
 * por TTL / exceso de longitud). El mensaje se confirma con ACK porque ya cumplió su
 * ciclo: su único destino es quedar auditado para inspección manual u operativa.
 */
@Component
@Slf4j
public class OrderDlqListener {

    @RabbitListener(queues = RabbitTopologyConfig.DLQ_SHIPMENT_DELIVERED)
    public void onShipmentDeliveredDlq(String payload, Channel channel,
                                       @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        log.error("[DLQ] {} mensaje no entregado: {}", RabbitTopologyConfig.DLQ_SHIPMENT_DELIVERED, payload);
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = RabbitTopologyConfig.DLQ_SHIPMENT_LOST)
    public void onShipmentLostDlq(String payload, Channel channel,
                                  @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        log.error("[DLQ] {} mensaje no entregado: {}", RabbitTopologyConfig.DLQ_SHIPMENT_LOST, payload);
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = RabbitTopologyConfig.DLQ_CUSTOMER_NOTIFY)
    public void onCustomerNotifyDlq(String payload, Channel channel,
                                    @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        log.error("[DLQ] {} mensaje no entregado: {}", RabbitTopologyConfig.DLQ_CUSTOMER_NOTIFY, payload);
        channel.basicAck(deliveryTag, false);
    }
}
