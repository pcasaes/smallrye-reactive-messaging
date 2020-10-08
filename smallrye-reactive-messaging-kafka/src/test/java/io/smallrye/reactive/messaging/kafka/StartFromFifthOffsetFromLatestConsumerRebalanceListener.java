package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
@Named("my-group-starting-on-fifth-happy-path")
public class StartFromFifthOffsetFromLatestConsumerRebalanceListener implements KafkaConsumerRebalanceListener {

    private final AtomicInteger rebalanceCount = new AtomicInteger();

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer,
        Collection<TopicPartition> partitions) {
        System.out.println("Assigning " + partitions + "  from " + Thread.currentThread().getName());
        if (! partitions.isEmpty()) {
            rebalanceCount.incrementAndGet();
            Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions);
            for (Map.Entry<TopicPartition, Long> position : offsets.entrySet()) {
                consumer.seek(position.getKey(), Math.max(0L, position.getValue() - 5L));
            }
        }
    }

    public int getRebalanceCount() {
        return rebalanceCount.get();
    }
}
