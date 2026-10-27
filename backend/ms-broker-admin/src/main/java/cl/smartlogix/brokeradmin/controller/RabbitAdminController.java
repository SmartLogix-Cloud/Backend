package cl.smartlogix.brokeradmin.controller;

import cl.smartlogix.brokeradmin.admin.RabbitAdminService;
import cl.smartlogix.brokeradmin.admin.dto.AdminResult;
import cl.smartlogix.brokeradmin.admin.dto.CreateBindingRequest;
import cl.smartlogix.brokeradmin.admin.dto.CreateExchangeRequest;
import cl.smartlogix.brokeradmin.admin.dto.CreateQueueRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * API REST de administracion de topologia de RabbitMQ.
 * Permite crear y eliminar colas, exchanges y bindings. La logica concreta vive
 * en {@link RabbitAdminService}; este controlador solo valida y traduce a HTTP.
 */
@RestController
@RequiredArgsConstructor
@Tag(name = "Administracion RabbitMQ", description = "Gestion de colas, exchanges y bindings")
public class RabbitAdminController {

    private final RabbitAdminService adminService;

    // ===================== Colas =====================

    @Operation(summary = "Crear cola")
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Cola creada"),
        @ApiResponse(responseCode = "400", description = "Parametros invalidos")
    })
    @PostMapping("/queues")
    public ResponseEntity<AdminResult> createQueue(@Valid @RequestBody CreateQueueRequest req) {
        String name = adminService.createQueue(req.name(), req.durableOrDefault());
        return ResponseEntity.status(HttpStatus.CREATED)
            .body(new AdminResult("queue", name, "created", true));
    }

    @Operation(summary = "Eliminar cola")
    @ApiResponse(responseCode = "200", description = "Resultado de la eliminacion")
    @DeleteMapping("/queues/{name}")
    public ResponseEntity<AdminResult> deleteQueue(@PathVariable String name) {
        boolean deleted = adminService.deleteQueue(name);
        return ResponseEntity.ok(new AdminResult("queue", name, "deleted", deleted));
    }

    // ===================== Exchanges =====================

    @Operation(summary = "Crear exchange")
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Exchange creado"),
        @ApiResponse(responseCode = "400", description = "Parametros invalidos")
    })
    @PostMapping("/exchanges")
    public ResponseEntity<AdminResult> createExchange(@Valid @RequestBody CreateExchangeRequest req) {
        adminService.createExchange(req.name(), req.type(), req.durableOrDefault());
        return ResponseEntity.status(HttpStatus.CREATED)
            .body(new AdminResult("exchange", req.name(), "created", true));
    }

    @Operation(summary = "Eliminar exchange")
    @ApiResponse(responseCode = "200", description = "Resultado de la eliminacion")
    @DeleteMapping("/exchanges/{name}")
    public ResponseEntity<AdminResult> deleteExchange(@PathVariable String name) {
        boolean deleted = adminService.deleteExchange(name);
        return ResponseEntity.ok(new AdminResult("exchange", name, "deleted", deleted));
    }

    // ===================== Bindings =====================

    @Operation(summary = "Crear binding cola-exchange")
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Binding creado"),
        @ApiResponse(responseCode = "400", description = "Parametros invalidos")
    })
    @PostMapping("/bindings")
    public ResponseEntity<AdminResult> createBinding(@Valid @RequestBody CreateBindingRequest req) {
        adminService.createBinding(req.queue(), req.exchange(), req.routingKeyOrEmpty());
        return ResponseEntity.status(HttpStatus.CREATED)
            .body(new AdminResult("binding", req.exchange() + "->" + req.queue(), "bound", true));
    }

    @Operation(summary = "Eliminar binding cola-exchange")
    @ApiResponse(responseCode = "200", description = "Binding eliminado")
    @DeleteMapping("/bindings")
    public ResponseEntity<AdminResult> removeBinding(@Valid @RequestBody CreateBindingRequest req) {
        adminService.removeBinding(req.queue(), req.exchange(), req.routingKeyOrEmpty());
        return ResponseEntity.ok(new AdminResult("binding", req.exchange() + "->" + req.queue(), "unbound", true));
    }
}
