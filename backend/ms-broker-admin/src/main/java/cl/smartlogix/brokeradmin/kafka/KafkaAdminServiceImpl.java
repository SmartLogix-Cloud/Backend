package cl.smartlogix.brokeradmin.kafka;

import cl.smartlogix.brokeradmin.kafka.dto.CreateTopicRequest;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.BrokerInfo;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.ClusterInfo;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.ConsumerGroupDetail;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.ConsumerGroupSummary;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.MemberInfo;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.PartitionInfo;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.PartitionLag;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.TopicDetail;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.TopicSummary;
import cl.smartlogix.brokeradmin.kafka.dto.ResetOffsetsRequest;
import cl.smartlogix.brokeradmin.kafka.dto.UpdateTopicConfigRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementacion sobre {@link AdminClient}. Cada operacion espera el resultado con
 * timeout y traduce las excepciones de Kafka a {@link KafkaAdminException} con el
 * codigo HTTP adecuado (404, 409, 400, 504, 502).
 */
@Service
@Slf4j
public class KafkaAdminServiceImpl implements KafkaAdminService {

    private final AdminClient admin;
    private final long timeoutMs;

    public KafkaAdminServiceImpl(AdminClient admin,
                                 @Value("${smartlogix.kafka.admin.timeout-ms:10000}") long timeoutMs) {
        this.admin = admin;
        this.timeoutMs = timeoutMs;
    }

    // ===================== Cluster =====================

    @Override
    public ClusterInfo describeCluster() {
        DescribeClusterResult result = admin.describeCluster();
        Node controller = await(result.controller(), "describir el cluster");
        List<BrokerInfo> brokers = await(result.nodes(), "describir el cluster").stream()
            .sorted(Comparator.comparingInt(Node::id))
            .map(n -> new BrokerInfo(n.id(), n.host(), n.port(), n.rack(), controller != null && n.id() == controller.id()))
            .toList();
        return new ClusterInfo(await(result.clusterId(), "describir el cluster"),
            controller != null ? controller.id() : -1, brokers);
    }

    // ===================== Topicos =====================

    @Override
    public List<TopicSummary> listTopics() {
        var names = await(admin.listTopics().names(), "listar topicos");
        if (names.isEmpty()) {
            return List.of();
        }
        return await(admin.describeTopics(names).allTopicNames(), "listar topicos").values().stream()
            .sorted(Comparator.comparing(TopicDescription::name))
            .map(d -> new TopicSummary(d.name(), d.partitions().size(), replicationFactor(d)))
            .toList();
    }

    @Override
    public TopicDetail describeTopic(String name) {
        TopicDescription description = await(admin.describeTopics(List.of(name)).allTopicNames(),
            "describir el topico " + name).get(name);

        List<TopicPartition> tps = description.partitions().stream()
            .map(p -> new TopicPartition(name, p.partition())).toList();
        Map<TopicPartition, ListOffsetsResultInfo> earliest = offsets(tps, OffsetSpec.earliest());
        Map<TopicPartition, ListOffsetsResultInfo> latest = offsets(tps, OffsetSpec.latest());

        List<PartitionInfo> partitions = new ArrayList<>();
        long messages = 0;
        for (TopicPartitionInfo p : description.partitions()) {
            TopicPartition tp = new TopicPartition(name, p.partition());
            long begin = earliest.get(tp).offset();
            long end = latest.get(tp).offset();
            messages += Math.max(0, end - begin);
            partitions.add(new PartitionInfo(
                p.partition(),
                p.leader() != null ? p.leader().id() : null,
                p.replicas().stream().map(Node::id).toList(),
                p.isr().stream().map(Node::id).toList(),
                p.isr().size() == p.replicas().size(),
                begin, end));
        }
        return new TopicDetail(name, partitions.size(), replicationFactor(description), messages,
            partitions, explicitConfigs(name));
    }

    @Override
    public TopicDetail createTopic(CreateTopicRequest req) {
        NewTopic topic = new NewTopic(req.name(), req.partitions(), req.replicationFactor());
        if (req.configs() != null && !req.configs().isEmpty()) {
            topic.configs(req.configs());
        }
        await(admin.createTopics(List.of(topic)).all(), "crear el topico " + req.name());
        log.info("Topico creado: {} (particiones={}, replicas={}, configs={})",
            req.name(), req.partitions(), req.replicationFactor(), req.configs());
        return describeTopic(req.name());
    }

    @Override
    public Map<String, String> updateTopicConfig(String name, UpdateTopicConfigRequest req) {
        List<AlterConfigOp> ops = new ArrayList<>();
        if (req.set() != null) {
            req.set().forEach((k, v) -> ops.add(new AlterConfigOp(new ConfigEntry(k, v), AlterConfigOp.OpType.SET)));
        }
        if (req.delete() != null) {
            req.delete().forEach(k -> ops.add(new AlterConfigOp(new ConfigEntry(k, null), AlterConfigOp.OpType.DELETE)));
        }
        if (ops.isEmpty()) {
            throw new KafkaAdminException(HttpStatus.BAD_REQUEST, "Debe indicar al menos una configuracion en 'set' o 'delete'");
        }
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        await(admin.incrementalAlterConfigs(Map.of(resource, ops)).all(), "modificar la configuracion de " + name);
        log.info("Configuracion de {} modificada: set={} delete={}", name, req.set(), req.delete());
        return explicitConfigs(name);
    }

    @Override
    public TopicDetail increasePartitions(String name, int totalPartitions) {
        await(admin.createPartitions(Map.of(name, NewPartitions.increaseTo(totalPartitions))).all(),
            "aumentar las particiones de " + name);
        log.info("Topico {} ahora tiene {} particiones", name, totalPartitions);
        return describeTopic(name);
    }

    @Override
    public void deleteTopic(String name) {
        await(admin.deleteTopics(List.of(name)).all(), "eliminar el topico " + name);
        log.info("Topico eliminado: {}", name);
    }

    // ===================== Consumer groups =====================

    @Override
    public List<ConsumerGroupSummary> listConsumerGroups() {
        return await(admin.listGroups(ListGroupsOptions.forConsumerGroups()).all(), "listar consumer groups").stream()
            .map(g -> new ConsumerGroupSummary(g.groupId(),
                g.groupState().map(GroupState::toString).orElse("UNKNOWN")))
            .sorted(Comparator.comparing(ConsumerGroupSummary::groupId))
            .toList();
    }

    @Override
    public ConsumerGroupDetail describeConsumerGroup(String groupId) {
        ConsumerGroupDescription group = describeGroup(groupId);

        List<MemberInfo> members = group.members().stream()
            .map(m -> new MemberInfo(m.consumerId(), m.clientId(), m.host(),
                m.assignment().topicPartitions().stream()
                    .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                    .map(TopicPartition::toString).toList()))
            .toList();

        Map<TopicPartition, OffsetAndMetadata> committed = await(
            admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
            "leer los offsets del grupo " + groupId);
        committed.values().removeIf(java.util.Objects::isNull);

        Map<TopicPartition, ListOffsetsResultInfo> ends = committed.isEmpty() ? Map.of()
            : offsets(new ArrayList<>(committed.keySet()), OffsetSpec.latest());

        List<PartitionLag> lags = committed.entrySet().stream()
            .map(e -> {
                long end = ends.get(e.getKey()).offset();
                long offset = e.getValue().offset();
                return new PartitionLag(e.getKey().topic(), e.getKey().partition(), offset, end, Math.max(0, end - offset));
            })
            .sorted(Comparator.comparing(PartitionLag::topic).thenComparingInt(PartitionLag::partition))
            .toList();
        long totalLag = lags.stream().mapToLong(PartitionLag::lag).sum();

        return new ConsumerGroupDetail(groupId, group.groupState().toString(), group.partitionAssignor(),
            group.coordinator() != null ? group.coordinator().id() : null, members, lags, totalLag);
    }

    @Override
    public ConsumerGroupDetail resetOffsets(String groupId, ResetOffsetsRequest req) {
        ConsumerGroupDescription group = describeGroup(groupId);
        if (group.groupState() != GroupState.EMPTY && group.groupState() != GroupState.DEAD) {
            throw new KafkaAdminException(HttpStatus.CONFLICT, "El grupo " + groupId + " esta " + group.groupState()
                + ": detenga sus consumidores antes de reposicionar offsets");
        }
        if (req.strategy() == ResetOffsetsRequest.Strategy.TO_OFFSET && req.offset() == null) {
            throw new KafkaAdminException(HttpStatus.BAD_REQUEST, "La estrategia TO_OFFSET requiere 'offset'");
        }

        TopicDescription topic = await(admin.describeTopics(List.of(req.topic())).allTopicNames(),
            "describir el topico " + req.topic()).get(req.topic());
        List<TopicPartition> tps = topic.partitions().stream()
            .map(p -> new TopicPartition(req.topic(), p.partition())).toList();

        Map<TopicPartition, OffsetAndMetadata> target = switch (req.strategy()) {
            case EARLIEST -> toOffsets(offsets(tps, OffsetSpec.earliest()));
            case LATEST -> toOffsets(offsets(tps, OffsetSpec.latest()));
            case TO_OFFSET -> tps.stream().collect(Collectors.toMap(Function.identity(),
                tp -> new OffsetAndMetadata(req.offset())));
        };
        await(admin.alterConsumerGroupOffsets(groupId, target).all(), "reposicionar offsets de " + groupId);
        log.info("Offsets del grupo {} en {} reposicionados con estrategia {}", groupId, req.topic(), req.strategy());
        return describeConsumerGroup(groupId);
    }

    @Override
    public void deleteConsumerGroup(String groupId) {
        await(admin.deleteConsumerGroups(List.of(groupId)).all(), "eliminar el grupo " + groupId);
        log.info("Consumer group eliminado: {}", groupId);
    }

    // ===================== Helpers =====================

    private ConsumerGroupDescription describeGroup(String groupId) {
        ConsumerGroupDescription group = await(admin.describeConsumerGroups(List.of(groupId)).all(),
            "describir el grupo " + groupId).get(groupId);
        if (group == null || group.groupState() == GroupState.DEAD && group.members().isEmpty()
                && await(admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
                    "leer offsets de " + groupId).isEmpty()) {
            throw new KafkaAdminException(HttpStatus.NOT_FOUND, "Consumer group no encontrado: " + groupId);
        }
        return group;
    }

    private Map<String, String> explicitConfigs(String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = await(admin.describeConfigs(List.of(resource)).all(),
            "leer la configuracion de " + topic).get(resource);
        Map<String, String> result = new TreeMap<>();
        config.entries().stream()
            .filter(e -> e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
            .forEach(e -> result.put(e.name(), e.value()));
        return result;
    }

    private Map<TopicPartition, ListOffsetsResultInfo> offsets(List<TopicPartition> tps, OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        tps.forEach(tp -> request.put(tp, spec));
        return await(admin.listOffsets(request).all(), "leer offsets");
    }

    private static Map<TopicPartition, OffsetAndMetadata> toOffsets(Map<TopicPartition, ListOffsetsResultInfo> infos) {
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        infos.forEach((tp, info) -> result.put(tp, new OffsetAndMetadata(info.offset())));
        return result;
    }

    private static int replicationFactor(TopicDescription d) {
        return d.partitions().isEmpty() ? 0 : d.partitions().getFirst().replicas().size();
    }

    private <T> T await(KafkaFuture<T> future, String operation) {
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException(HttpStatus.SERVICE_UNAVAILABLE, "Operacion interrumpida al " + operation, e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new KafkaAdminException(HttpStatus.GATEWAY_TIMEOUT, "Kafka no respondio a tiempo al " + operation, e);
        } catch (ExecutionException e) {
            throw translate(e.getCause(), operation);
        }
    }

    private static KafkaAdminException translate(Throwable cause, String operation) {
        String detail = "No se pudo " + operation + ": " + cause.getMessage();
        HttpStatus status;
        if (cause instanceof UnknownTopicOrPartitionException || cause instanceof GroupIdNotFoundException) {
            status = HttpStatus.NOT_FOUND;
        } else if (cause instanceof TopicExistsException || cause instanceof GroupNotEmptyException) {
            status = HttpStatus.CONFLICT;
        } else if (cause instanceof InvalidPartitionsException || cause instanceof InvalidReplicationFactorException
            || cause instanceof InvalidConfigurationException || cause instanceof InvalidTopicException
            || cause instanceof InvalidRequestException) {
            status = HttpStatus.BAD_REQUEST;
        } else if (cause instanceof TimeoutException) {
            status = HttpStatus.GATEWAY_TIMEOUT;
        } else {
            status = HttpStatus.BAD_GATEWAY;
        }
        return new KafkaAdminException(status, detail, cause);
    }
}
