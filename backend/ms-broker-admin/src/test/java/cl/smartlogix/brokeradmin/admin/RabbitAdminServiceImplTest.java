package cl.smartlogix.brokeradmin.admin;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RabbitAdminServiceImplTest {

    private final AmqpAdmin amqpAdmin = mock(AmqpAdmin.class);
    private final RabbitAdminServiceImpl service = new RabbitAdminServiceImpl(amqpAdmin);

    @Test
    void createQueueShouldDeclareDurableQueue() {
        when(amqpAdmin.declareQueue(any(Queue.class))).thenReturn("pagos.q");

        String name = service.createQueue("pagos.q", true);

        assertEquals("pagos.q", name);
        verify(amqpAdmin).declareQueue(any(Queue.class));
    }

    @Test
    void deleteQueueShouldDelegate() {
        when(amqpAdmin.deleteQueue("pagos.q")).thenReturn(true);

        assertTrue(service.deleteQueue("pagos.q"));
        verify(amqpAdmin).deleteQueue("pagos.q");
    }

    @Test
    void createExchangeShouldBuildByType() {
        service.createExchange("pagos.ex", "topic", true);
        verify(amqpAdmin).declareExchange(any(Exchange.class));
    }

    @Test
    void createExchangeShouldRejectUnsupportedType() {
        assertThrows(IllegalArgumentException.class,
            () -> service.createExchange("pagos.ex", "banana", true));
        verify(amqpAdmin, never()).declareExchange(any());
    }

    @Test
    void createBindingShouldDeclareBinding() {
        service.createBinding("pagos.q", "pagos.ex", "pago.creado");
        verify(amqpAdmin).declareBinding(any(Binding.class));
    }

    @Test
    void removeBindingShouldDelegate() {
        service.removeBinding("pagos.q", "pagos.ex", "pago.creado");
        verify(amqpAdmin).removeBinding(any(Binding.class));
    }
}
