package cl.smartlogix.shipping.messaging;

import cl.smartlogix.shipping.messaging.event.OrderCancelledEvent;
import cl.smartlogix.shipping.messaging.event.OrderReadyForShipmentEvent;
import cl.smartlogix.shipping.service.ShipmentService;
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
 * Consumidores del dominio Envio: reaccionan a los eventos de pedido. Usan ACK
 * manual: confirman con basicAck en caso de exito y hacen basicNack sin requeue
 * (enviando a la DLQ) ante error.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class OrderEventsListener {

    private final ShipmentService shipmentService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = RabbitTopologyConfig.Q_ORDER_READY)
    public void onOrderReady(String payload, Channel channel,
                             @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            OrderReadyForShipmentEvent event = objectMapper.readValue(payload, OrderReadyForShipmentEvent.class);
            shipmentService.onOrderReadyForShipment(event.orderId(), event.orderCode());
            channel.basicAck(deliveryTag, false);
            log.info("Procesado order.ready-for-shipment para pedido {}", event.orderId());
        } catch (Exception e) {
            log.error("Fallo procesando order.ready-for-shipment, enviando a DLQ. Payload: {}", payload, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }

    @RabbitListener(queues = RabbitTopologyConfig.Q_ORDER_CANCELLED)
    public void onOrderCancelled(String payload, Channel channel,
                                 @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            OrderCancelledEvent event = objectMapper.readValue(payload, OrderCancelledEvent.class);
            shipmentService.onOrderCancelled(event.orderId());
            channel.basicAck(deliveryTag, false);
            log.info("Procesado order.cancelled para pedido {}", event.orderId());
        } catch (Exception e) {
            log.error("Fallo procesando order.cancelled, enviando a DLQ. Payload: {}", payload, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
