package cl.smartlogix.order.messaging;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
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
 *
 * <p>Se usan DOS tipos de exchange:
 * <ul>
 *   <li><b>direct</b> ({@value #COMMANDS_EXCHANGE}) para COMANDOS (ordenes de hacer algo,
 *       enrutamiento 1-a-1 por coincidencia exacta de routing key). ms-order PUBLICA
 *       los comandos {@code order.ready-for-shipment} y {@code order.cancelled}.</li>
 *   <li><b>topic</b> ({@value #EVENTS_EXCHANGE}) para EVENTOS (hechos ya ocurridos,
 *       enrutamiento por patron). ms-order CONSUME los eventos {@code shipment.*}.</li>
 * </ul>
 */
@Configuration
public class RabbitTopologyConfig {

    // ===================== Nombres centralizados =====================
    /** Exchange DIRECT de comandos (enrutamiento exacto por routing key). */
    public static final String COMMANDS_EXCHANGE = "smartlogix.commands";
    /** Exchange TOPIC de eventos (enrutamiento por patron). */
    public static final String EVENTS_EXCHANGE = "smartlogix.events";
    /** Dead-letter exchange comun para los mensajes no entregados. */
    public static final String DEAD_LETTER_EXCHANGE = "smartlogix.dlx";

    // Routing keys que ms-order PUBLICA como COMANDOS (al exchange direct)
    public static final String RK_ORDER_READY = "order.ready-for-shipment";
    public static final String RK_ORDER_CANCELLED = "order.cancelled";

    // Routing keys que ms-order CONSUME como EVENTOS (del exchange topic)
    public static final String RK_SHIPMENT_DELIVERED = "shipment.delivered";
    public static final String RK_SHIPMENT_LOST = "shipment.lost";

    // Colas de ms-order (dominio Pedido) y sus dead-letter queues
    public static final String Q_SHIPMENT_DELIVERED = "order.shipment-delivered.q";
    public static final String Q_SHIPMENT_LOST = "order.shipment-lost.q";
    public static final String DLQ_SHIPMENT_DELIVERED = "order.shipment-delivered.dlq";
    public static final String DLQ_SHIPMENT_LOST = "order.shipment-lost.dlq";

    // Cola de NOTIFICACION al cliente: consume TODOS los eventos de envio (shipment.*)
    // del exchange topic. Es un consumidor distinto al de dominio: demuestra el
    // fan-out del topic (un mismo evento llega a varios consumidores).
    public static final String Q_CUSTOMER_NOTIFY = "order.customer-notify.q";
    public static final String DLQ_CUSTOMER_NOTIFY = "order.customer-notify.dlq";
    /** Patron topic que captura todos los eventos de envio para notificar. */
    public static final String RK_SHIPMENT_ALL = "shipment.#";
    /** Routing key con la que los mensajes fallidos de notificacion llegan a su DLQ. */
    public static final String RK_NOTIFY_DEAD = "notify.dead";

    // ===================== Politicas de retencion / limpieza =====================
    /** TTL del mensaje: a los 5 min sin consumir, caduca y pasa a la DLQ. */
    private static final int MESSAGE_TTL_MS = 300_000;
    /** Limite de mensajes en cola: al excederse se descarta la cabeza (dead-letter). */
    private static final int MAX_LENGTH = 10_000;

    // ===================== Exchanges =====================
    @Bean
    DirectExchange commandsExchange() {
        return ExchangeBuilder.directExchange(COMMANDS_EXCHANGE).durable(true).build();
    }

    @Bean
    TopicExchange eventsExchange() {
        return ExchangeBuilder.topicExchange(EVENTS_EXCHANGE).durable(true).build();
    }

    @Bean
    TopicExchange deadLetterExchange() {
        return ExchangeBuilder.topicExchange(DEAD_LETTER_EXCHANGE).durable(true).build();
    }

    // ===================== Colas principales (con DLQ + retencion) =====================
    @Bean
    Queue shipmentDeliveredQueue() {
        return mainQueue(Q_SHIPMENT_DELIVERED, RK_SHIPMENT_DELIVERED);
    }

    @Bean
    Queue shipmentLostQueue() {
        return mainQueue(Q_SHIPMENT_LOST, RK_SHIPMENT_LOST);
    }

    /** Cola principal durable con dead-lettering y politicas de retencion (TTL + limite). */
    private Queue mainQueue(String name, String deadLetterRoutingKey) {
        return QueueBuilder.durable(name)
            .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
            .withArgument("x-dead-letter-routing-key", deadLetterRoutingKey)
            .withArgument("x-message-ttl", MESSAGE_TTL_MS)
            .withArgument("x-max-length", MAX_LENGTH)
            .withArgument("x-overflow", "drop-head")
            .build();
    }

    @Bean
    Queue customerNotifyQueue() {
        return mainQueue(Q_CUSTOMER_NOTIFY, RK_NOTIFY_DEAD);
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

    @Bean
    Queue customerNotifyDlq() {
        return QueueBuilder.durable(DLQ_CUSTOMER_NOTIFY).build();
    }

    // ===================== Bindings principales (eventos shipment.* al topic) =====================
    @Bean
    Binding bindShipmentDelivered() {
        return BindingBuilder.bind(shipmentDeliveredQueue()).to(eventsExchange()).with(RK_SHIPMENT_DELIVERED);
    }

    @Bean
    Binding bindShipmentLost() {
        return BindingBuilder.bind(shipmentLostQueue()).to(eventsExchange()).with(RK_SHIPMENT_LOST);
    }

    // La cola de notificacion escucha TODOS los eventos de envio (patron topic shipment.#)
    @Bean
    Binding bindCustomerNotify() {
        return BindingBuilder.bind(customerNotifyQueue()).to(eventsExchange()).with(RK_SHIPMENT_ALL);
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

    @Bean
    Binding bindCustomerNotifyDlq() {
        return BindingBuilder.bind(customerNotifyDlq()).to(deadLetterExchange()).with(RK_NOTIFY_DEAD);
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
