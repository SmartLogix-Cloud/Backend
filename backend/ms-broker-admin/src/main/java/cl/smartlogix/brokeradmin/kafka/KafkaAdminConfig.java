package cl.smartlogix.brokeradmin.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * Cliente de administracion de Kafka. Reutiliza la configuracion de conexion de
 * {@link KafkaAdmin} (spring.kafka.*) y se cierra al apagar la aplicacion.
 */
@Configuration
public class KafkaAdminConfig {

    @Bean(destroyMethod = "close")
    AdminClient kafkaAdminClient(KafkaAdmin kafkaAdmin) {
        Map<String, Object> props = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        props.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.putIfAbsent(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        return AdminClient.create(props);
    }
}
