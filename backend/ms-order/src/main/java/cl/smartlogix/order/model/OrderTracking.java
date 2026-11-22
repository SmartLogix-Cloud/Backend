package cl.smartlogix.order.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.OffsetDateTime;

/**
 * Proyeccion local del seguimiento del envio de un pedido, alimentada en tiempo
 * real por los eventos Kafka de ms-shipping. Evita consultar ms-shipping de forma
 * sincrona para conocer el estado logistico de un pedido.
 */
@Entity
@Table(name = "pedido_seguimiento")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderTracking {

    @Id
    @Column(name = "id_pedido")
    private Long orderId;

    @Column(name = "id_envio", nullable = false)
    private Long shipmentId;

    @Column(name = "tracking_number", length = 60)
    private String trackingNumber;

    @Column(name = "transportista", length = 120)
    private String carrierName;

    @Column(name = "estado_envio", nullable = false, length = 30)
    private String shipmentStatus;

    @Column(name = "ultima_ubicacion", length = 255)
    private String lastLocation;

    @Column(name = "ultimo_comentario", length = 500)
    private String lastComment;

    @Column(name = "ultimo_evento_id", nullable = false, length = 64)
    private String lastEventId;

    @Column(name = "ultimo_evento_at", nullable = false)
    private OffsetDateTime lastEventAt;

    @Column(name = "eventos_recibidos", nullable = false)
    private int eventsReceived;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @PrePersist
    @PreUpdate
    void touch() {
        this.updatedAt = OffsetDateTime.now();
    }
}
