package cl.smartlogix.brokeradmin.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
            .info(new Info()
                .title("ms-broker-admin — API SmartLogix")
                .version("1.0.0")
                .description("Microservicio administrador de RabbitMQ: creacion y eliminacion de colas, exchanges y bindings.")
                .contact(new Contact()
                    .name("Equipo Backend SmartLogix")
                    .email("soporte@smartlogix.cl"))
                .license(new License().name("MIT")));
    }
}
