package cl.smartlogix.shipping.messaging;

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
 * Configuracion centralizada de la mensajeria de ms-shipping (dominio Envio).
 *
 * <p>Declara como beans y constantes toda la topologia de RabbitMQ (exchange,
 * colas, DLQ, routing keys y bindings) del dominio. La logica de negocio referencia
 * estas constantes y no contiene literales de mensajeria.
 */
@Configuration
public class RabbitTopologyConfig {

    // ===================== Nombres centralizados =====================
    public static final String LOGISTICS_EXCHANGE = "smartlogix.logistics";
    public static final String DEAD_LETTER_EXCHANGE = "smartlogix.logistics.dlx";

    // Routing keys que ms-shipping PUBLICA
    public static final String RK_SHIPMENT_DELIVERED = "shipment.delivered";
    public static final String RK_SHIPMENT_LOST = "shipment.lost";

    // Routing keys que ms-shipping CONSUME (provenientes de ms-order)
    public static final String RK_ORDER_READY = "order.ready-for-shipment";
    public static final String RK_ORDER_CANCELLED = "order.cancelled";

    // Colas de ms-shipping (dominio Envio) y sus dead-letter queues
    public static final String Q_ORDER_READY = "shipping.order-ready.q";
    public static final String Q_ORDER_CANCELLED = "shipping.order-cancelled.q";
    public static final String DLQ_ORDER_READY = "shipping.order-ready.dlq";
    public static final String DLQ_ORDER_CANCELLED = "shipping.order-cancelled.dlq";

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
    Queue orderReadyQueue() {
        return QueueBuilder.durable(Q_ORDER_READY)
            .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
            .withArgument("x-dead-letter-routing-key", RK_ORDER_READY)
            .build();
    }

    @Bean
    Queue orderCancelledQueue() {
        return QueueBuilder.durable(Q_ORDER_CANCELLED)
            .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
            .withArgument("x-dead-letter-routing-key", RK_ORDER_CANCELLED)
            .build();
    }

    // ===================== Dead-letter queues =====================
    @Bean
    Queue orderReadyDlq() {
        return QueueBuilder.durable(DLQ_ORDER_READY).build();
    }

    @Bean
    Queue orderCancelledDlq() {
        return QueueBuilder.durable(DLQ_ORDER_CANCELLED).build();
    }

    // ===================== Bindings principales =====================
    @Bean
    Binding bindOrderReady() {
        return BindingBuilder.bind(orderReadyQueue()).to(logisticsExchange()).with(RK_ORDER_READY);
    }

    @Bean
    Binding bindOrderCancelled() {
        return BindingBuilder.bind(orderCancelledQueue()).to(logisticsExchange()).with(RK_ORDER_CANCELLED);
    }

    // ===================== Bindings de las DLQ =====================
    @Bean
    Binding bindOrderReadyDlq() {
        return BindingBuilder.bind(orderReadyDlq()).to(deadLetterExchange()).with(RK_ORDER_READY);
    }

    @Bean
    Binding bindOrderCancelledDlq() {
        return BindingBuilder.bind(orderCancelledDlq()).to(deadLetterExchange()).with(RK_ORDER_CANCELLED);
    }

    // ===================== Listener factory con ACK manual =====================
    @Bean
    SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setDefaultRequeueRejected(false);
        return factory;
    }
}
