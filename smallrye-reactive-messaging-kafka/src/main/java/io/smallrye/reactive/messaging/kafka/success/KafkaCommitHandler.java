package io.smallrye.reactive.messaging.kafka.success;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.core.Context;

public interface KafkaCommitHandler {

    default void partitionsAssigned(Context context, Set<TopicPartition> partitions) {

    }

    <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record);

}
