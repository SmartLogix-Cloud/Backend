package cl.smartlogix.brokeradmin.kafka;

import cl.smartlogix.brokeradmin.kafka.dto.CreateTopicRequest;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.ClusterInfo;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.ConsumerGroupDetail;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.ConsumerGroupSummary;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.TopicDetail;
import cl.smartlogix.brokeradmin.kafka.dto.KafkaViews.TopicSummary;
import cl.smartlogix.brokeradmin.kafka.dto.ResetOffsetsRequest;
import cl.smartlogix.brokeradmin.kafka.dto.UpdateTopicConfigRequest;

import java.util.List;
import java.util.Map;

/**
 * Operaciones de administracion de la plataforma Kafka: cluster, topicos,
 * configuraciones, particiones y consumer groups.
 */
public interface KafkaAdminService {

    ClusterInfo describeCluster();

    List<TopicSummary> listTopics();

    TopicDetail describeTopic(String name);

    TopicDetail createTopic(CreateTopicRequest request);

    Map<String, String> updateTopicConfig(String name, UpdateTopicConfigRequest request);

    TopicDetail increasePartitions(String name, int totalPartitions);

    void deleteTopic(String name);

    List<ConsumerGroupSummary> listConsumerGroups();

    ConsumerGroupDetail describeConsumerGroup(String groupId);

    ConsumerGroupDetail resetOffsets(String groupId, ResetOffsetsRequest request);

    void deleteConsumerGroup(String groupId);
}
