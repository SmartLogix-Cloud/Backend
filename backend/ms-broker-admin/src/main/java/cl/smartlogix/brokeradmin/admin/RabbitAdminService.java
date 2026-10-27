package cl.smartlogix.brokeradmin.admin;

/**
 * Servicio dedicado que encapsula TODA la logica de administracion de RabbitMQ
 * (crear/eliminar colas, exchanges y bindings). Los controladores llaman a estos
 * metodos de alto nivel y no conocen detalles internos del broker.
 */
public interface RabbitAdminService {

    /** Crea (o declara idempotentemente) una cola. */
    String createQueue(String name, boolean durable);

    /** Elimina una cola. Devuelve true si existia y fue eliminada. */
    boolean deleteQueue(String name);

    /** Crea un exchange del tipo indicado (direct, topic, fanout, headers). */
    void createExchange(String name, String type, boolean durable);

    /** Elimina un exchange. Devuelve true si existia y fue eliminado. */
    boolean deleteExchange(String name);

    /** Enlaza una cola a un exchange con la routingKey dada. */
    void createBinding(String queue, String exchange, String routingKey);

    /** Elimina el binding cola-exchange con la routingKey dada. */
    void removeBinding(String queue, String exchange, String routingKey);
}
