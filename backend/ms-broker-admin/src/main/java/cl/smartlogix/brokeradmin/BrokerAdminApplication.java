package cl.smartlogix.brokeradmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Microservicio administrador de RabbitMQ.
 *
 * <p>Expone una API REST para crear y eliminar colas, exchanges y bindings en el
 * broker, sin participar de la mensajería de negocio. Toda la interacción con
 * RabbitMQ se encapsula en {@code RabbitAdminService}.
 */
@SpringBootApplication
public class BrokerAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(BrokerAdminApplication.class, args);
    }
}
