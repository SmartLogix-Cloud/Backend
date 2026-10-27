package cl.smartlogix.shipping.service;

import cl.smartlogix.shipping.dto.UpdateShipmentRStatusRequest;
import cl.smartlogix.shipping.dto.AssingCarierRequest;
import cl.smartlogix.shipping.dto.CreateShipmentRequest;
import cl.smartlogix.shipping.dto.ShipmentDto;
import cl.smartlogix.shipping.dto.ShipmentTrackingDto;
import cl.smartlogix.shipping.messaging.ShipmentEventsPublisher;
import cl.smartlogix.shipping.model.Shipment;
import cl.smartlogix.shipping.model.ShipmentTracking;
import cl.smartlogix.shipping.model.ShipmentState;
import cl.smartlogix.shipping.model.Carrier;
import cl.smartlogix.shipping.repository.ShipmentRepository;
import cl.smartlogix.shipping.repository.ShipmentTrackingRepository;
import cl.smartlogix.shipping.repository.CarrierRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class ShipmentServiceImpl implements ShipmentService {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    // Máquina de estados del envío
    private static final Map<ShipmentState, Set<ShipmentState>> TRANSITIONS = Map.of(
        ShipmentState.LISTO_PARA_RETIRO, Set.of(ShipmentState.RETIRADO, ShipmentState.PERDIDO),
        ShipmentState.RETIRADO,          Set.of(ShipmentState.EN_TRANSITO, ShipmentState.PERDIDO),
        ShipmentState.EN_TRANSITO,       Set.of(ShipmentState.ENTREGADO, ShipmentState.RETRASADO,
                                                ShipmentState.ENTREGA_FALLIDA, ShipmentState.PERDIDO),
        ShipmentState.RETRASADO,         Set.of(ShipmentState.EN_TRANSITO, ShipmentState.ENTREGADO,
                                                ShipmentState.ENTREGA_FALLIDA, ShipmentState.PERDIDO),
        ShipmentState.ENTREGA_FALLIDA,   Set.of(ShipmentState.EN_TRANSITO, ShipmentState.PERDIDO),
        ShipmentState.ENTREGADO,         Set.of(),
        ShipmentState.PERDIDO,           Set.of()
    );

    private final ShipmentRepository shipmentRepository;
    private final CarrierRepository carrierRepository;
    private final ShipmentTrackingRepository trackingRepository;
    private final ShipmentEventsPublisher eventsPublisher;

    @Override
    public ShipmentDto create(CreateShipmentRequest req) {
        Shipment shipment = Shipment.builder()
            .orderId(req.orderId())
            .status(ShipmentState.LISTO_PARA_RETIRO)
            .destinationAddress(req.destinationAddress())
            .district(req.district())
            .region(req.region())
            .estimatedDate(req.estimatedDate())
            .trackingNumber(generateTracking())
            .build();
        shipment.addTracking(ShipmentTracking.builder()
            .status(ShipmentState.LISTO_PARA_RETIRO)
            .comment("Envío creado, listo para retiro")
            .build());
        return ShipmentDto.from(shipmentRepository.save(shipment));
    }

    @Override
    @Transactional(readOnly = true)
    public ShipmentDto findById(Long id) {
        return ShipmentDto.from(find(id));
    }

    @Override
    @Transactional(readOnly = true)
    public ShipmentDto findByTracking(String trackingNumber) {
        Shipment s = shipmentRepository.findByTrackingNumber(trackingNumber)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Tracking no encontrado: " + trackingNumber));
        return ShipmentDto.from(s);
    }

    @Override
    @Transactional(readOnly = true)
    public List<ShipmentDto> findByOrder(Long orderId) {
        return shipmentRepository.findByOrderId(orderId).stream().map(ShipmentDto::from).toList();
    }

    @Override
    @Transactional(readOnly = true)
    public List<ShipmentDto> findAll() {
        return shipmentRepository.findAll().stream().map(ShipmentDto::from).toList();
    }

    @Override
    @Transactional(readOnly = true)
    public List<ShipmentDto> findByStatus(ShipmentState status) {
        return shipmentRepository.findByStatus(status).stream().map(ShipmentDto::from).toList();
    }

    @Override
    @Transactional(readOnly = true)
    public List<ShipmentTrackingDto> history(Long shipmentId) {
        return trackingRepository.findByShipment_IdOrderByCreatedAtAsc(shipmentId).stream()
            .map(ShipmentTrackingDto::from).toList();
    }

    @Override
    public ShipmentDto assignCarrier(Long shipmentId, AssingCarierRequest req) {
        Shipment shipment = find(shipmentId);
        if (shipment.getStatus() != ShipmentState.LISTO_PARA_RETIRO) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                "Solo se puede asignar transportista en estado LISTO_PARA_RETIRO (actual: " + shipment.getStatus() + ")");
        }
        Carrier c = carrierRepository.findById(req.carrierId())
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                "Transportista no encontrado: " + req.carrierId()));
        if (!Boolean.TRUE.equals(c.getActive())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Transportista inactivo: " + c.getName());
        }

        shipment.setCarrier(c);
        shipment.addTracking(ShipmentTracking.builder()
            .status(ShipmentState.LISTO_PARA_RETIRO)
            .comment("Transportista asignado: " + c.getName())
            .build());
        return ShipmentDto.from(shipmentRepository.save(shipment));
    }

    @Override
    public ShipmentDto changeStatus(Long shipmentId, UpdateShipmentRStatusRequest req) {
        Shipment shipment = find(shipmentId);
        ShipmentState current = shipment.getStatus();
        ShipmentState next = req.status();

        if (!TRANSITIONS.getOrDefault(current, Set.of()).contains(next)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                "Transición no permitida: " + current + " → " + next);
        }

        shipment.setStatus(next);
        if (next == ShipmentState.ENTREGADO) {
            shipment.setDeliveryDate(OffsetDateTime.now());
        }
        shipment.addTracking(ShipmentTracking.builder()
            .status(next)
            .location(req.location())
            .comment(req.comment())
            .build());
        Shipment saved = shipmentRepository.save(shipment);

        // Publicación de hitos hacia ms-order.
        if (next == ShipmentState.ENTREGADO) {
            eventsPublisher.publishDelivered(saved.getOrderId(), saved.getTrackingNumber());
        } else if (next == ShipmentState.PERDIDO) {
            eventsPublisher.publishLost(saved.getOrderId(), saved.getTrackingNumber(), req.comment());
        }

        return ShipmentDto.from(saved);
    }

    @Override
    public void onOrderReadyForShipment(Long orderId, String orderCode) {
        if (!shipmentRepository.findByOrderId(orderId).isEmpty()) {
            log.info("order.ready-for-shipment ignorado: el pedido {} ya tiene envío", orderId);
            return; // idempotente
        }
        Shipment shipment = Shipment.builder()
            .orderId(orderId)
            .status(ShipmentState.LISTO_PARA_RETIRO)
            .destinationAddress("Pedido " + orderCode + " — dirección por confirmar")
            .trackingNumber(generateTracking())
            .build();
        shipment.addTracking(ShipmentTracking.builder()
            .status(ShipmentState.LISTO_PARA_RETIRO)
            .comment("Envío generado automáticamente desde el pedido " + orderCode)
            .build());
        shipmentRepository.save(shipment);
        log.info("Envío creado automáticamente para el pedido {}", orderId);
    }

    @Override
    public void onOrderCancelled(Long orderId) {
        List<Shipment> shipments = shipmentRepository.findByOrderId(orderId);
        for (Shipment shipment : shipments) {
            if (shipment.getStatus() == ShipmentState.LISTO_PARA_RETIRO) {
                shipment.addTracking(ShipmentTracking.builder()
                    .status(ShipmentState.LISTO_PARA_RETIRO)
                    .comment("Pedido cancelado: no despachar")
                    .build());
                shipmentRepository.save(shipment);
                log.info("Envío {} marcado como no despachar por cancelación del pedido {}",
                    shipment.getId(), orderId);
            }
        }
    }

    private Shipment find(Long id) {
        return shipmentRepository.findById(id)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Envío no encontrado: " + id));
    }

    private String generateTracking() {
        String date = LocalDate.now().format(DATE_FORMAT);
        String suffix = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return "ENV-" + date + "-" + suffix;
    }
}
