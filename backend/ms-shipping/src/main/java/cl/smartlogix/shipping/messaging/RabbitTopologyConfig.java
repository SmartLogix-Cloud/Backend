package cl.smartlogix.shipping.messaging;

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
 * Configuracion centralizada de la mensajeria de ms-shipping (dominio Envio).
 *
 * <p>Declara como beans y constantes toda la topologia de RabbitMQ (exchanges,
 * colas, DLQ, routing keys y bindings) del dominio. La logica de negocio referencia
 * estas constantes y no contiene literales de mensajeria.
 *
 * <p>Se usan DOS tipos de exchange:
 * <ul>
 *   <li><b>direct</b> ({@value #COMMANDS_EXCHANGE}) para COMANDOS: ms-shipping CONSUME
 *       los comandos {@code order.ready-for-shipment} y {@code order.cancelled} que le
 *       ordenan crear o cancelar un envio (enrutamiento exacto por routing key).</li>
 *   <li><b>topic</b> ({@value #EVENTS_EXCHANGE}) para EVENTOS: ms-shipping PUBLICA los
 *       hechos {@code shipment.delivered} y {@code shipment.lost}.</li>
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

    // Routing keys que ms-shipping PUBLICA como EVENTOS (al exchange topic)
    public static final String RK_SHIPMENT_DELIVERED = "shipment.delivered";
    public static final String RK_SHIPMENT_LOST = "shipment.lost";

    // Routing keys que ms-shipping CONSUME como COMANDOS (del exchange direct)
    public static final String RK_ORDER_READY = "order.ready-for-shipment";
    public static final String RK_ORDER_CANCELLED = "order.cancelled";

    // Colas de ms-shipping (dominio Envio) y sus dead-letter queues
    public static final String Q_ORDER_READY = "shipping.order-ready.q";
    public static final String Q_ORDER_CANCELLED = "shipping.order-cancelled.q";
    public static final String DLQ_ORDER_READY = "shipping.order-ready.dlq";
    public static final String DLQ_ORDER_CANCELLED = "shipping.order-cancelled.dlq";

    // Cola de AUDITORIA: consume TODOS los eventos (patron topic '#') del exchange
    // de eventos para dejar registro. Demuestra, junto a la notificacion, el fan-out
    // del topic: un mismo evento se enruta a varios consumidores a la vez.
    public static final String Q_AUDIT = "shipping.audit.q";
    public static final String DLQ_AUDIT = "shipping.audit.dlq";
    /** Patron topic que captura TODOS los eventos para auditar. */
    public static final String RK_ALL_EVENTS = "#";
    /** Routing key con la que los mensajes fallidos de auditoria llegan a su DLQ. */
    public static final String RK_AUDIT_DEAD = "audit.dead";

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
    Queue orderReadyQueue() {
        return mainQueue(Q_ORDER_READY, RK_ORDER_READY);
    }

    @Bean
    Queue orderCancelledQueue() {
        return mainQueue(Q_ORDER_CANCELLED, RK_ORDER_CANCELLED);
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
    Queue auditQueue() {
        return mainQueue(Q_AUDIT, RK_AUDIT_DEAD);
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

    @Bean
    Queue auditDlq() {
        return QueueBuilder.durable(DLQ_AUDIT).build();
    }

    // ===================== Bindings principales (comandos order.* al direct) =====================
    @Bean
    Binding bindOrderReady() {
        return BindingBuilder.bind(orderReadyQueue()).to(commandsExchange()).with(RK_ORDER_READY);
    }

    @Bean
    Binding bindOrderCancelled() {
        return BindingBuilder.bind(orderCancelledQueue()).to(commandsExchange()).with(RK_ORDER_CANCELLED);
    }

    // La cola de auditoria escucha TODOS los eventos del topic (patron '#')
    @Bean
    Binding bindAudit() {
        return BindingBuilder.bind(auditQueue()).to(eventsExchange()).with(RK_ALL_EVENTS);
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

    @Bean
    Binding bindAuditDlq() {
        return BindingBuilder.bind(auditDlq()).to(deadLetterExchange()).with(RK_AUDIT_DEAD);
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
