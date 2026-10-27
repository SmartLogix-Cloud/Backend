package cl.smartlogix.shipping.controller;

import cl.smartlogix.shipping.dto.*;
import cl.smartlogix.shipping.model.ShipmentState;
import cl.smartlogix.shipping.service.ShipmentService;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ShipmentControllerTest {

    private ShipmentDto shipment() {

        return new ShipmentDto(
                1L,
                100L,
                null,
                null,
                "ENV-001",
                ShipmentState.LISTO_PARA_RETIRO,
                "Direccion",
                "Puente Alto",
                "RM",
                null,
                null,
                List.of(),
                OffsetDateTime.now(),
                OffsetDateTime.now()
        );
    }

    @Test
    void createShouldReturnCreated() {

        ShipmentService service = mock(ShipmentService.class);

        ShipmentController controller =
                new ShipmentController(service);

        CreateShipmentRequest request =
                new CreateShipmentRequest(
                        100L,
                        "Direccion",
                        "Puente Alto",
                        "RM",
                        null
                );

        when(service.create(request))
                .thenReturn(shipment());

        ResponseEntity<ShipmentDto> response =
                controller.create(request);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        verify(service).create(request);
    }

    @Test
    void getByIdShouldReturnShipment() {

        ShipmentService service = mock(ShipmentService.class);

        ShipmentController controller =
                new ShipmentController(service);

        when(service.findById(1L))
                .thenReturn(shipment());

        ResponseEntity<ShipmentDto> response =
                controller.getById(1L);

        assertEquals(HttpStatus.OK, response.getStatusCode());

        verify(service).findById(1L);
    }

    @Test
    void getByTrackingShouldReturnShipment() {

        ShipmentService service = mock(ShipmentService.class);

        ShipmentController controller =
                new ShipmentController(service);

        when(service.findByTracking("ENV-001"))
                .thenReturn(shipment());

        controller.getByTracking("ENV-001");

        verify(service).findByTracking("ENV-001");
    }

    @Test
    void getByOrderShouldReturnShipments() {

        ShipmentService service = mock(ShipmentService.class);

        ShipmentController controller =
                new ShipmentController(service);

        when(service.findByOrder(100L))
                .thenReturn(List.of(shipment()));

        controller.getByOrder(100L);

        verify(service).findByOrder(100L);
    }

    @Test
    void listShouldReturnAll() {

        ShipmentService service = mock(ShipmentService.class);

        ShipmentController controller =
                new ShipmentController(service);

        when(service.findAll())
                .thenReturn(List.of());

        controller.list(null);

        verify(service).findAll();
    }

    @Test
    void listShouldFilterByStatus() {

        ShipmentService service = mock(ShipmentService.class);

        ShipmentController controller =
                new ShipmentController(service);

        when(service.findByStatus(ShipmentState.LISTO_PARA_RETIRO))
                .thenReturn(List.of());

        controller.list(ShipmentState.LISTO_PARA_RETIRO);

        verify(service).findByStatus(ShipmentState.LISTO_PARA_RETIRO);
    }

    @Test
    void historyShouldReturnTrackingHistory() {

        ShipmentService service = mock(ShipmentService.class);

        ShipmentController controller =
                new ShipmentController(service);

        when(service.history(1L))
                .thenReturn(List.of());

        controller.history(1L);

        verify(service).history(1L);
    }

    @Test
    void assignCarrierShouldInvokeService() {

        ShipmentService service = mock(ShipmentService.class);

        ShipmentController controller =
                new ShipmentController(service);

        AssingCarierRequest request =
                new AssingCarierRequest(10L);

        when(service.assignCarrier(1L, request))
                .thenReturn(shipment());

        controller.assignCarrier(1L, request);

        verify(service).assignCarrier(1L, request);
    }

    @Test
    void changeStatusShouldInvokeService() {

        ShipmentService service = mock(ShipmentService.class);

        ShipmentController controller =
                new ShipmentController(service);

        UpdateShipmentRStatusRequest request =
                new UpdateShipmentRStatusRequest(
                        ShipmentState.EN_TRANSITO,
                        "Santiago",
                        "En camino"
                );

        when(service.changeStatus(1L, request))
                .thenReturn(shipment());

        controller.changeStatus(1L, request);

        verify(service).changeStatus(1L, request);
    }
}