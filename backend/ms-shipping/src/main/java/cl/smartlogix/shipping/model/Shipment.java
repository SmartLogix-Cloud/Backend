package cl.smartlogix.shipping.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "envios", indexes = {
    @Index(name = "idx_envios_pedido", columnList = "id_pedido")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Shipment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_envio")
    private Long id;

    /** ID lógico del pedido en ms-order — NO es FK física. */
    @Column(name = "id_pedido", nullable = false)
    private Long orderId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "id_transportista")
    private Carrier carrier;

    @Column(name = "tracking_number", unique = true, length = 60)
    private String trackingNumber;

    @Enumerated(EnumType.STRING)
    @Column(name = "estado", nullable = false, length = 30)
    private ShipmentState status = ShipmentState.LISTO_PARA_RETIRO;

    @Column(name = "direccion_destino", nullable = false, length = 255)
    private String destinationAddress;

    @Column(name = "comuna", length = 120)
    private String district;

    @Column(length = 120)
    private String region;

    @Column(name = "fecha_estimada")
    private LocalDate estimatedDate;

    @Column(name = "fecha_entrega")
    private OffsetDateTime deliveryDate;

    @OneToMany(mappedBy = "shipment", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @Builder.Default
    private List<ShipmentTracking> tracking = new ArrayList<>();

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @PrePersist
    void onCreate() {
        OffsetDateTime now = OffsetDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        this.updatedAt = OffsetDateTime.now();
    }

    public void addTracking(ShipmentTracking t) {
        t.setShipment(this);
        this.tracking.add(t);
    }
}
