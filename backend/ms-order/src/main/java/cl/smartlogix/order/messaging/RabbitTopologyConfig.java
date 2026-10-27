package cl.smartlogix.order.messaging;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuracion centralizada de la mensajeria de ms-order (dominio Pedido).
 *
 * <p>Toda la topologia de RabbitMQ (nombres de exchange, colas, DLQ, routing keys
 * y bindings) se declara aqui como beans y constantes. La logica de negocio NO
 * contiene literales de mensajeria: referencia estas constantes.
 */
@Configuration
public class RabbitTopologyConfig {

    // ===================== Nombres centralizados =====================
    public static final String LOGISTICS_EXCHANGE = "smartlogix.logistics";
    public static final String DEAD_LETTER_EXCHANGE = "smartlogix.logistics.dlx";

    // Routing keys que ms-order PUBLICA
    public static final String RK_ORDER_READY = "order.ready-for-shipment";
    public static final String RK_ORDER_CANCELLED = "order.cancelled";

    // Routing keys que ms-order CONSUME (provenientes de ms-shipping)
    public static final String RK_SHIPMENT_DELIVERED = "shipment.delivered";
    public static final String RK_SHIPMENT_LOST = "shipment.lost";

    // Colas de ms-order (dominio Pedido) y sus dead-letter queues
    public static final String Q_SHIPMENT_DELIVERED = "order.shipment-delivered.q";
    public static final String Q_SHIPMENT_LOST = "order.shipment-lost.q";
    public static final String DLQ_SHIPMENT_DELIVERED = "order.shipment-delivered.dlq";
    public static final String DLQ_SHIPMENT_LOST = "order.shipment-lost.dlq";

    // ===================== Exchanges =====================
    @Bean
    TopicExchange logisticsExchange() {
        return ExchangeBuilder.topicExchange(LOGISTICS_EXCHANGE).durable(true).build();
    }

    @Bean
    TopicExchange deadLetterExchange() {
        return ExchangeBuilder.topicExchange(DEAD_LETTER_EXCHANGE).durable(true).build();
    }

    // ===================== Colas principales (con DLQ) =====================
    @Bean
    Queue shipmentDeliveredQueue() {
        return QueueBuilder.durable(Q_SHIPMENT_DELIVERED)
            .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
            .withArgument("x-dead-letter-routing-key", RK_SHIPMENT_DELIVERED)
            .build();
    }

    @Bean
    Queue shipmentLostQueue() {
        return QueueBuilder.durable(Q_SHIPMENT_LOST)
            .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
            .withArgument("x-dead-letter-routing-key", RK_SHIPMENT_LOST)
            .build();
    }

    // ===================== Dead-letter queues =====================
    @Bean
    Queue shipmentDeliveredDlq() {
        return QueueBuilder.durable(DLQ_SHIPMENT_DELIVERED).build();
    }

    @Bean
    Queue shipmentLostDlq() {
        return QueueBuilder.durable(DLQ_SHIPMENT_LOST).build();
    }

    // ===================== Bindings principales =====================
    @Bean
    Binding bindShipmentDelivered() {
        return BindingBuilder.bind(shipmentDeliveredQueue()).to(logisticsExchange()).with(RK_SHIPMENT_DELIVERED);
    }

    @Bean
    Binding bindShipmentLost() {
        return BindingBuilder.bind(shipmentLostQueue()).to(logisticsExchange()).with(RK_SHIPMENT_LOST);
    }

    // ===================== Bindings de las DLQ =====================
    @Bean
    Binding bindShipmentDeliveredDlq() {
        return BindingBuilder.bind(shipmentDeliveredDlq()).to(deadLetterExchange()).with(RK_SHIPMENT_DELIVERED);
    }

    @Bean
    Binding bindShipmentLostDlq() {
        return BindingBuilder.bind(shipmentLostDlq()).to(deadLetterExchange()).with(RK_SHIPMENT_LOST);
    }

    // ===================== Listener factory con ACK manual =====================
    /**
     * Sustituye la factory por defecto de Spring Boot para forzar ACK MANUAL en
     * todos los {@code @RabbitListener}: el consumidor decide explicitamente entre
     * basicAck (exito) y basicNack sin requeue (error -> DLQ).
     */
    @Bean
    SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setDefaultRequeueRejected(false);
        return factory;
    }
}
