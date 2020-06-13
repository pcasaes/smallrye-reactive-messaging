package io.smallrye.reactive.messaging.kafka;

import java.util.Set;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import io.smallrye.mutiny.Uni;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

@ApplicationScoped
@Named("StartFromFifthOffsetFromLatestConsumerRebalanceListener")
public class StartFromFifthOffsetFromLatestConsumerRebalanceListener implements KafkaConsumerRebalanceListener {

    @Override
    public Uni<Void> onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> set) {
        consumer.pause();

        return Uni
                .combine()
                .all()
                .unis(set
                        .stream()
                        .map(topicPartition -> consumer.endOffsets(topicPartition)
                                .onItem()
                                .produceUni(o -> consumer.seek(topicPartition, Math.max(0L, o - 5L))))
                        .collect(Collectors.toList()))
                .combinedWith(a -> null)
                .onItemOrFailure()
                .apply((a, t) -> {
                    consumer.resume();
                    return null;
                });
    }

    @Override
    public Uni<Void> onPartitionsRevoked(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions) {
        return Uni
                .createFrom()
                .nullItem();
    }
}
