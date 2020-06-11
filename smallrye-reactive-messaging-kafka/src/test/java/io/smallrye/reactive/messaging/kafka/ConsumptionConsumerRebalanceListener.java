package io.smallrye.reactive.messaging.kafka;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

@ApplicationScoped
@Named("ConsumptionConsumerRebalanceListener")
public class ConsumptionConsumerRebalanceListener implements KafkaConsumerRebalanceListener {

    private final Map<Integer, TopicPartition> assigned = new ConcurrentHashMap<>();

    @Override
    public void onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> set) {
        set.forEach(topicPartition -> this.assigned.put(topicPartition.getPartition(), topicPartition));
    }

    @Override
    public void onPartitionsRevoked(KafkaConsumer<?, ?> consumer, Set<TopicPartition> set) {

    }

    public Map<Integer, TopicPartition> getAssigned() {
        return assigned;
    }
}
