package cl.smartlogix.order.controller;

import cl.smartlogix.order.dto.OrderTrackingDto;
import cl.smartlogix.order.service.OrderTrackingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Consulta del seguimiento logistico de un pedido (proyeccion alimentada por Kafka). */
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
@Tag(name = "Seguimiento", description = "Estado logistico del pedido actualizado por streaming (Kafka)")
public class OrderTrackingController {

    private final OrderTrackingService trackingService;

    @Operation(summary = "Seguimiento del pedido",
        description = "Ultimo estado del envio del pedido, actualizado en tiempo real por eventos Kafka de ms-shipping.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Seguimiento encontrado"),
        @ApiResponse(responseCode = "404", description = "El pedido aun no tiene eventos de envio")
    })
    @GetMapping("/{id}/tracking")
    public OrderTrackingDto tracking(@PathVariable Long id) {
        return trackingService.findByOrder(id);
    }
}
