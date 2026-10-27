package cl.smartlogix.brokeradmin.admin;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Implementacion de {@link RabbitAdminService} apoyada en {@link AmqpAdmin}
 * (RabbitAdmin), el componente de Spring AMQP que declara y elimina topologia
 * directamente en el broker.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RabbitAdminServiceImpl implements RabbitAdminService {

    private final AmqpAdmin amqpAdmin;

    @Override
    public String createQueue(String name, boolean durable) {
        String declared = amqpAdmin.declareQueue(new Queue(name, durable));
        log.info("Cola declarada: {} (durable={})", name, durable);
        return declared != null ? declared : name;
    }

    @Override
    public boolean deleteQueue(String name) {
        boolean deleted = amqpAdmin.deleteQueue(name);
        log.info("Cola eliminada: {} (existia={})", name, deleted);
        return deleted;
    }

    @Override
    public void createExchange(String name, String type, boolean durable) {
        Exchange exchange = buildExchange(name, type, durable);
        amqpAdmin.declareExchange(exchange);
        log.info("Exchange declarado: {} (type={}, durable={})", name, type, durable);
    }

    @Override
    public boolean deleteExchange(String name) {
        boolean deleted = amqpAdmin.deleteExchange(name);
        log.info("Exchange eliminado: {} (existia={})", name, deleted);
        return deleted;
    }

    @Override
    public void createBinding(String queue, String exchange, String routingKey) {
        amqpAdmin.declareBinding(binding(queue, exchange, routingKey));
        log.info("Binding creado: {} -> {} [{}]", exchange, queue, routingKey);
    }

    @Override
    public void removeBinding(String queue, String exchange, String routingKey) {
        amqpAdmin.removeBinding(binding(queue, exchange, routingKey));
        log.info("Binding eliminado: {} -> {} [{}]", exchange, queue, routingKey);
    }

    private Exchange buildExchange(String name, String type, boolean durable) {
        return switch (type) {
            case "direct"  -> ExchangeBuilder.directExchange(name).durable(durable).build();
            case "topic"   -> ExchangeBuilder.topicExchange(name).durable(durable).build();
            case "fanout"  -> ExchangeBuilder.fanoutExchange(name).durable(durable).build();
            case "headers" -> ExchangeBuilder.headersExchange(name).durable(durable).build();
            default -> throw new IllegalArgumentException("Tipo de exchange no soportado: " + type);
        };
    }

    private Binding binding(String queue, String exchange, String routingKey) {
        return new Binding(queue, Binding.DestinationType.QUEUE, exchange, routingKey, Map.of());
    }
}
