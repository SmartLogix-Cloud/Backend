package cl.smartlogix.shipping.service;

import cl.smartlogix.shipping.dto.AssingCarierRequest;
import cl.smartlogix.shipping.dto.CreateShipmentRequest;
import cl.smartlogix.shipping.dto.UpdateShipmentRStatusRequest;
import cl.smartlogix.shipping.messaging.ShipmentEventsPublisher;
import cl.smartlogix.shipping.model.Carrier;
import cl.smartlogix.shipping.model.Shipment;
import cl.smartlogix.shipping.model.ShipmentState;
import cl.smartlogix.shipping.model.ShipmentTracking;
import cl.smartlogix.shipping.repository.CarrierRepository;
import cl.smartlogix.shipping.repository.ShipmentRepository;
import cl.smartlogix.shipping.repository.ShipmentTrackingRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ShipmentServiceImplTest {

    private ShipmentRepository shipmentRepository;
    private CarrierRepository carrierRepository;
    private ShipmentTrackingRepository trackingRepository;
    private ShipmentEventsPublisher eventsPublisher;

    private ShipmentServiceImpl service;

    @BeforeEach
    void setup() {

        shipmentRepository = mock(ShipmentRepository.class);
        carrierRepository = mock(CarrierRepository.class);
        trackingRepository = mock(ShipmentTrackingRepository.class);
        eventsPublisher = mock(ShipmentEventsPublisher.class);

        service = new ShipmentServiceImpl(
                shipmentRepository,
                carrierRepository,
                trackingRepository,
                eventsPublisher
        );
    }

    @Test
    void shouldCreateShipment() {

        CreateShipmentRequest request =
                new CreateShipmentRequest(
                        10L,
                        "Av. Providencia 100",
                        "Providencia",
                        "RM",
                        LocalDate.now().plusDays(2)
                );

        when(shipmentRepository.save(any(Shipment.class)))
                .thenAnswer(i -> {
                    Shipment s = i.getArgument(0);
                    s.setId(1L);
                    return s;
                });

        var result = service.create(request);

        assertNotNull(result);
        assertEquals(10L, result.orderId());
        assertEquals(ShipmentState.LISTO_PARA_RETIRO, result.status());

        verify(shipmentRepository).save(any());
    }

    @Test
    void shouldFindById() {

        Shipment shipment = Shipment.builder()
                .id(1L)
                .orderId(10L)
                .destinationAddress("Destino")
                .status(ShipmentState.LISTO_PARA_RETIRO)
                .build();

        when(shipmentRepository.findById(1L))
                .thenReturn(Optional.of(shipment));

        var result = service.findById(1L);

        assertEquals(1L, result.shipmentId());
    }

    @Test
    void shouldThrowWhenShipmentNotFound() {

        when(shipmentRepository.findById(1L))
                .thenReturn(Optional.empty());

        assertThrows(
                ResponseStatusException.class,
                () -> service.findById(1L)
        );
    }

    @Test
    void shouldFindByTracking() {

        Shipment shipment = Shipment.builder()
                .id(1L)
                .trackingNumber("ENV-001")
                .destinationAddress("Destino")
                .status(ShipmentState.LISTO_PARA_RETIRO)
                .build();

        when(shipmentRepository.findByTrackingNumber("ENV-001"))
                .thenReturn(Optional.of(shipment));

        assertEquals(
                "ENV-001",
                service.findByTracking("ENV-001").trackingNumber()
        );
    }

    @Test
    void shouldFindAll() {

        when(shipmentRepository.findAll())
                .thenReturn(
                        List.of(
                                Shipment.builder()
                                        .id(1L)
                                        .destinationAddress("A")
                                        .status(ShipmentState.LISTO_PARA_RETIRO)
                                        .build()
                        )
                );

        assertEquals(1, service.findAll().size());
    }

    @Test
    void shouldFindByStatus() {

        when(shipmentRepository.findByStatus(ShipmentState.LISTO_PARA_RETIRO))
                .thenReturn(
                        List.of(
                                Shipment.builder()
                                        .id(1L)
                                        .destinationAddress("A")
                                        .status(ShipmentState.LISTO_PARA_RETIRO)
                                        .build()
                        )
                );

        assertEquals(
                1,
                service.findByStatus(ShipmentState.LISTO_PARA_RETIRO).size()
        );
    }

    @Test
    void shouldFindByOrder() {

        when(shipmentRepository.findByOrderId(10L))
                .thenReturn(
                        List.of(
                                Shipment.builder()
                                        .id(1L)
                                        .orderId(10L)
                                        .destinationAddress("A")
                                        .status(ShipmentState.LISTO_PARA_RETIRO)
                                        .build()
                        )
                );

        assertEquals(
                1,
                service.findByOrder(10L).size()
        );
    }

    @Test
    void shouldReturnHistory() {

        ShipmentTracking tracking =
                ShipmentTracking.builder()
                        .id(1L)
                        .status(ShipmentState.LISTO_PARA_RETIRO)
                        .comment("Creado")
                        .build();

        when(
                trackingRepository.findByShipment_IdOrderByCreatedAtAsc(1L)
        ).thenReturn(List.of(tracking));

        assertEquals(
                1,
                service.history(1L).size()
        );
    }

    @Test
    void shouldAssignCarrier() {

        Carrier carrier =
                Carrier.builder()
                        .id(1L)
                        .name("Chilexpress")
                        .active(true)
                        .build();

        Shipment shipment =
                Shipment.builder()
                        .id(1L)
                        .destinationAddress("Destino")
                        .status(ShipmentState.LISTO_PARA_RETIRO)
                        .build();

        when(shipmentRepository.findById(1L))
                .thenReturn(Optional.of(shipment));

        when(carrierRepository.findById(1L))
                .thenReturn(Optional.of(carrier));

        when(shipmentRepository.save(any()))
                .thenAnswer(i -> i.getArgument(0));

        var result =
                service.assignCarrier(
                        1L,
                        new AssingCarierRequest(1L)
                );

        assertEquals(
                ShipmentState.LISTO_PARA_RETIRO,
                result.status()
        );
    }

    @Test
    void shouldFailAssignCarrierWhenShipmentNotCreated() {

        Shipment shipment =
                Shipment.builder()
                        .id(1L)
                        .destinationAddress("Destino")
                        .status(ShipmentState.EN_TRANSITO)
                        .build();

        when(shipmentRepository.findById(1L))
                .thenReturn(Optional.of(shipment));

        assertThrows(
                ResponseStatusException.class,
                () -> service.assignCarrier(
                        1L,
                        new AssingCarierRequest(1L)
                )
        );
    }

    @Test
    void shouldChangeStatus() {

        Shipment shipment =
                Shipment.builder()
                        .id(1L)
                        .destinationAddress("Destino")
                        .status(ShipmentState.RETIRADO)
                        .build();

        when(shipmentRepository.findById(1L))
                .thenReturn(Optional.of(shipment));

        when(shipmentRepository.save(any()))
                .thenAnswer(i -> i.getArgument(0));

        var result =
                service.changeStatus(
                        1L,
                        new UpdateShipmentRStatusRequest(
                                ShipmentState.EN_TRANSITO,
                                "Santiago",
                                "En tránsito"
                        )
                );

        assertEquals(
                ShipmentState.EN_TRANSITO,
                result.status()
        );
    }

    @Test
    void shouldRejectInvalidTransition() {

        Shipment shipment =
                Shipment.builder()
                        .id(1L)
                        .destinationAddress("Destino")
                        .status(ShipmentState.LISTO_PARA_RETIRO)
                        .build();

        when(shipmentRepository.findById(1L))
                .thenReturn(Optional.of(shipment));

        assertThrows(
                ResponseStatusException.class,
                () -> service.changeStatus(
                        1L,
                        new UpdateShipmentRStatusRequest(
                                ShipmentState.ENTREGADO,
                                null,
                                null
                        )
                )
        );
    }
}