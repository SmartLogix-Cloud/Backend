package cl.smartlogix.analytics.config;

import io.swagger.v3.oas.models.OpenAPI;
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
                .title("ms-analytics — API SmartLogix")
                .version("1.0.0")
                .description("Analitica logistica en tiempo real alimentada por los topicos Kafka "
                    + "smartlogix.order-events y smartlogix.shipment-events.")
                .license(new License().name("MIT")));
    }
}
