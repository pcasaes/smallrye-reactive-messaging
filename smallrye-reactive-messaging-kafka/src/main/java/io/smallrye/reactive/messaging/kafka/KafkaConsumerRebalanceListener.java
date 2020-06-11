package io.smallrye.reactive.messaging.kafka;

import java.util.Set;

import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

/**
 *
 * When implemented by a managed bean annotated with {@link javax.inject.Named} and
 * configured against a consumer, ex:
 * mp.messaging.incoming.example.consumer-rebalance-listener.name=ExampleConsumerRebalanceListener
 *
 * Will be applied as a consumer rebalance listener to the consumer.
 *
 * For more details
 *
 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener
 */
public interface KafkaConsumerRebalanceListener {

    /**
     * Called when the consumer is assigned topic partitions
     *
     * @param consumer underlying consumer
     * @param topicPartitions set of assigned topic partitions
     */
    void onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions);

    /**
     * Called when the consumer is revoked topic partitions
     *
     * @param consumer underlying consumer
     * @param topicPartitions set of revoked topic partitions
     */
    void onPartitionsRevoked(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions);
}
