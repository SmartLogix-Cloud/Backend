package cl.smartlogix.order.messaging;

import cl.smartlogix.order.messaging.event.ShipmentDeliveredEvent;
import cl.smartlogix.order.messaging.event.ShipmentLostEvent;
import cl.smartlogix.order.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidores del dominio Pedido: reaccionan a los eventos de envio que cambian
 * la maquina de estados del pedido. Usan ACK manual: confirman con basicAck en
 * caso de exito y hacen basicNack sin requeue (enviando a la DLQ) ante error.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ShipmentEventsListener {

    private final OrderService orderService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = RabbitTopologyConfig.Q_SHIPMENT_DELIVERED)
    public void onShipmentDelivered(String payload, Channel channel,
                                    @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            ShipmentDeliveredEvent event = objectMapper.readValue(payload, ShipmentDeliveredEvent.class);
            orderService.onShipmentDelivered(event.orderId());
            channel.basicAck(deliveryTag, false);
            log.info("Procesado shipment.delivered para pedido {}", event.orderId());
        } catch (Exception e) {
            log.error("Fallo procesando shipment.delivered, enviando a DLQ. Payload: {}", payload, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }

    @RabbitListener(queues = RabbitTopologyConfig.Q_SHIPMENT_LOST)
    public void onShipmentLost(String payload, Channel channel,
                               @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            ShipmentLostEvent event = objectMapper.readValue(payload, ShipmentLostEvent.class);
            orderService.onShipmentLost(event.orderId(), event.reason());
            channel.basicAck(deliveryTag, false);
            log.info("Procesado shipment.lost para pedido {}", event.orderId());
        } catch (Exception e) {
            log.error("Fallo procesando shipment.lost, enviando a DLQ. Payload: {}", payload, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
