package inbound;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

@ApplicationScoped
@Named("rebalanced-example.rebalancer")
public class KafkaRebalancedConsumerRebalanceListener implements KafkaConsumerRebalanceListener {

    private static final Logger LOGGER = Logger.getLogger(KafkaRebalancedConsumerRebalanceListener.class.getName());

    /**
     * With this mapping we can keep track of which partitions have been set-up properly.
     */
    private final List<Long> offsetMap = new CopyOnWriteArrayList<>();

    /**
     * When receiving a list of partitions will search for the earliest offset within 10 minutes
     * and seek the consumer to it. These operations are asynchronous so the inbound connector
     * WILL continue to receive messages from the subscribed topic that MIGHT be older than 10 minutes.
     *
     * @param consumer underlying consumer
     * @param topicPartitions set of assigned topic partitions
     */
    @Override
    public void onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions) {
        long now = System.currentTimeMillis();
        long shouldStartAt = now - 600_000L; //10 minute ago

        topicPartitions
            .forEach(topicPartition -> {
                LOGGER.info("Assigned " + topicPartition);
                final int partition = topicPartition.getPartition();

                //clean the local offset map
                this.offsetMap.add(partition, null);
                consumer.offsetsForTimes(topicPartition, shouldStartAt)
                    .subscribe()
                    .with(offsetAndTimestamp -> {
                        LOGGER.info("Seeking to " + offsetAndTimestamp);
                        if (offsetAndTimestamp == null) {
                            // no messages found in the request time period found so set
                            // local offset map to the 0th offset
                            this.offsetMap.add(partition, 0L);
                        } else {
                            this.offsetMap.add(partition, offsetAndTimestamp.getOffset());
                            consumer
                                .seek(topicPartition, offsetAndTimestamp.getOffset())
                                .subscribe()
                                .with(v -> LOGGER.info("Seeked to " + offsetAndTimestamp));
                        }
                    });
            });
    }

    @Override
    public void onPartitionsRevoked(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions) {
        // nothing to do here
    }

    /**
     * Because the operations in {@link #onPartitionsAssigned(KafkaConsumer, Set)} are asynchronous we
     * MIGHT need a way to inform the inbound connector to ignore incoming messages until this listener
     * has done its job.
     *
     * @param message
     * @return true if the consumer has been properly set-up for the messages partition
     */
    public boolean isConsumerReady(IncomingKafkaRecord<?, ?> message) {
        int partition = message.getPartition();
        if (partition < this.offsetMap.size()) {
            Long offset = this.offsetMap.get(partition);
            return offset != null && offset <= message.getOffset();
        }
        return false;
    }
}
