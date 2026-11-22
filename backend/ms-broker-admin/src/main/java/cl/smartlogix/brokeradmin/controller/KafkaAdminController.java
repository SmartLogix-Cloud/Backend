package cl.smartlogix.brokeradmin.controller;

import cl.smartlogix.brokeradmin.kafka.KafkaAdminService;
import cl.smartlogix.brokeradmin.kafka.dto.CreateTopicRequest;
import cl.smartlogix.brokeradmin.kafka.dto.IncreasePartitionsRequest;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.ClusterInfo;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.ConsumerGroupDetail;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.ConsumerGroupSummary;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.KafkaAdminResult;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.TopicDetail;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.TopicSummary;
import cl.smartlogix.brokeradmin.kafka.dto.ResetOffsetsRequest;
import cl.smartlogix.brokeradmin.kafka.dto.UpdateTopicConfigRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * API REST de administracion de la plataforma Kafka: cluster, topicos
 * (crear / listar / describir / modificar configuracion / aumentar particiones /
 * eliminar) y consumer groups (listar / describir con lag / reposicionar offsets /
 * eliminar). La logica vive en {@link KafkaAdminService}.
 */
@RestController
@RequestMapping("/kafka")
@RequiredArgsConstructor
@Tag(name = "Administracion Kafka", description = "Gestion de topicos, configuraciones y consumer groups")
public class KafkaAdminController {

    private final KafkaAdminService kafka;

    // ===================== Cluster =====================

    @Operation(summary = "Describir cluster", description = "Id del cluster, controller y brokers.")
    @GetMapping("/cluster")
    public ClusterInfo cluster() {
        return kafka.describeCluster();
    }

    // ===================== Topicos =====================

    @Operation(summary = "Listar topicos")
    @GetMapping("/topics")
    public List<TopicSummary> listTopics() {
        return kafka.listTopics();
    }

    @Operation(summary = "Describir topico",
        description = "Particiones (lider, replicas, ISR, offsets), mensajes retenidos y configuracion explicita.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Detalle del topico"),
        @ApiResponse(responseCode = "404", description = "Topico inexistente")
    })
    @GetMapping("/topics/{name}")
    public TopicDetail describeTopic(@PathVariable String name) {
        return kafka.describeTopic(name);
    }

    @Operation(summary = "Crear topico", description = "Con particiones, factor de replicacion y configuraciones.")
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Topico creado"),
        @ApiResponse(responseCode = "400", description = "Parametros invalidos"),
        @ApiResponse(responseCode = "409", description = "El topico ya existe")
    })
    @PostMapping("/topics")
    public ResponseEntity<TopicDetail> createTopic(@Valid @RequestBody CreateTopicRequest req) {
        return ResponseEntity.status(HttpStatus.CREATED).body(kafka.createTopic(req));
    }

    @Operation(summary = "Modificar configuracion de topico",
        description = "Ej: {\"set\": {\"retention.ms\": \"86400000\"}, \"delete\": [\"segment.ms\"]}")
    @PutMapping("/topics/{name}/configs")
    public Map<String, String> updateConfig(@PathVariable String name, @RequestBody UpdateTopicConfigRequest req) {
        return kafka.updateTopicConfig(name, req);
    }

    @Operation(summary = "Aumentar particiones", description = "Kafka solo permite aumentar, nunca reducir.")
    @PostMapping("/topics/{name}/partitions")
    public TopicDetail increasePartitions(@PathVariable String name,
                                          @Valid @RequestBody IncreasePartitionsRequest req) {
        return kafka.increasePartitions(name, req.totalPartitions());
    }

    @Operation(summary = "Eliminar topico")
    @DeleteMapping("/topics/{name}")
    public KafkaAdminResult deleteTopic(@PathVariable String name) {
        kafka.deleteTopic(name);
        return new KafkaAdminResult("topic", name, "deleted", true, null);
    }

    // ===================== Consumer groups =====================

    @Operation(summary = "Listar consumer groups")
    @GetMapping("/consumer-groups")
    public List<ConsumerGroupSummary> listGroups() {
        return kafka.listConsumerGroups();
    }

    @Operation(summary = "Describir consumer group",
        description = "Estado, miembros y particiones asignadas, offsets comprometidos y lag por particion.")
    @GetMapping("/consumer-groups/{groupId}")
    public ConsumerGroupDetail describeGroup(@PathVariable String groupId) {
        return kafka.describeConsumerGroup(groupId);
    }

    @Operation(summary = "Reposicionar offsets",
        description = "EARLIEST / LATEST / TO_OFFSET sobre todas las particiones de un topico. Grupo inactivo.")
    @PostMapping("/consumer-groups/{groupId}/reset-offsets")
    public ConsumerGroupDetail resetOffsets(@PathVariable String groupId,
                                            @Valid @RequestBody ResetOffsetsRequest req) {
        return kafka.resetOffsets(groupId, req);
    }

    @Operation(summary = "Eliminar consumer group", description = "Solo grupos sin consumidores activos.")
    @DeleteMapping("/consumer-groups/{groupId}")
    public KafkaAdminResult deleteGroup(@PathVariable String groupId) {
        kafka.deleteConsumerGroup(groupId);
        return new KafkaAdminResult("consumer-group", groupId, "deleted", true, null);
    }
}
