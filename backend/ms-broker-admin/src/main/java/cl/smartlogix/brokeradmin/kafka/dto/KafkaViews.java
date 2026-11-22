package cl.smartlogix.brokeradmin.kafka.dto;

import java.util.List;
import java.util.Map;

/** Vistas de solo lectura del estado de la plataforma Kafka. */
public final class KafkaViews {

    private KafkaViews() {
    }

    public record BrokerInfo(int id, String host, int port, String rack, boolean controller) {}

    public record ClusterInfo(String clusterId, int controllerId, List<BrokerInfo> brokers) {}

    public record TopicSummary(String name, int partitions, int replicationFactor) {}

    /**
     * @param inSync true si todas las replicas estan sincronizadas (ISR = replicas)
     */
    public record PartitionInfo(int partition, Integer leader, List<Integer> replicas, List<Integer> isr,
                                boolean inSync, long beginningOffset, long endOffset) {}

    /**
     * @param configs solo configuraciones explicitas del topico (no los defaults del broker)
     * @param messages mensajes retenidos aproximados (suma de end - beginning)
     */
    public record TopicDetail(String name, int partitions, int replicationFactor, long messages,
                              List<PartitionInfo> partitionDetails, Map<String, String> configs) {}

    public record ConsumerGroupSummary(String groupId, String state) {}

    public record MemberInfo(String memberId, String clientId, String host, List<String> assignments) {}

    /**
     * @param lag mensajes pendientes de consumir = endOffset - committedOffset
     */
    public record PartitionLag(String topic, int partition, long committedOffset, long endOffset, long lag) {}

    public record ConsumerGroupDetail(String groupId, String state, String assignor, Integer coordinator,
                                      List<MemberInfo> members, List<PartitionLag> offsets, long totalLag) {}

    public record KafkaAdminResult(String resource, String name, String action, boolean ok, Object detail) {}
}
