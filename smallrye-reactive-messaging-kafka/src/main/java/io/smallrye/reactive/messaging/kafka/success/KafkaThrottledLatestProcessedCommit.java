package io.smallrye.reactive.messaging.kafka.success;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

public class KafkaThrottledLatestProcessedCommit implements KafkaCommitHandler {

    private static final long THROTTLE_TIME_IN_MILLIS = 5_000L;
    private static final Map<String, Map<Integer, TopicPartition>> TOPIC_PARTITIONS_CACHE = new ConcurrentHashMap<>();

    private final Map<TopicPartition, Long> smallestUncommittedOffsets = new HashMap<>();
    private final Map<TopicPartition, Set<Long>> processedOffsetSets = new HashMap<>();

    /**
     * We keep retrying indefinitely to load initial positions. If a second re-balance occurs we need to cancel
     * the current attempts.
     * <p>
     * We KEY against a random UUID because {@link Cancellable#cancel()} might not stop the subscriber right away.
     */
    private final Map<UUID, Cancellable> loadingInitialPositions = new HashMap<>();

    private final KafkaConsumer<?, ?> consumer;

    private volatile Context context;
    private long nextCommitTime;

    public KafkaThrottledLatestProcessedCommit(KafkaConsumer<?, ?> consumer) {
        this.consumer = consumer;
    }

    private <K, V> TopicPartition getTopicPartition(IncomingKafkaRecord<K, V> record) {
        return TOPIC_PARTITIONS_CACHE
                .computeIfAbsent(record.getTopic(), topic -> new ConcurrentHashMap<>())
                .computeIfAbsent(record.getPartition(), partition -> new TopicPartition(record.getTopic(), partition));
    }

    private Set<Long> getProcessedOffsetSet(TopicPartition topicPartition) {
        return processedOffsetSets
                .computeIfAbsent(topicPartition, t -> ConcurrentHashMap.newKeySet());
    }

    @Override
    public void partitionsAssigned(Context context, Set<TopicPartition> partitions) {
        this.context = context;

        // cancel any current loading attempt
        loadingInitialPositions
                .values()
                .forEach(Cancellable::cancel);
        loadingInitialPositions.clear();

        smallestUncommittedOffsets.clear();
        processedOffsetSets.clear();

        for (TopicPartition topicPartition : partitions) {
            UUID loadingId = UUID.randomUUID();
            loadingInitialPositions.put(loadingId, this.consumer.position(topicPartition)
                    .onFailure().invoke(t -> {
                        resetNextCommitTime();
                        log.unableToLoadInitialOffset(topicPartition.toString(), t);
                    })
                    .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(10)).indefinitely()
                    .subscribe()
                    .with(offset -> {
                        log.loadingInitialOffset(topicPartition.toString(), offset);
                        smallestUncommittedOffsets.put(topicPartition, offset);
                        loadingInitialPositions.remove(loadingId);
                    }));
        }
        resetNextCommitTime();
    }

    private void resetNextCommitTime() {
        this.nextCommitTime = System.currentTimeMillis() + THROTTLE_TIME_IN_MILLIS;
    }

    private Map<TopicPartition, Long> getLatestOffsetMapping() {
        Map<TopicPartition, Long> offsetsMapping = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> smallestUncommittedOffsetEntry : smallestUncommittedOffsets
                .entrySet()) {
            TopicPartition topicPartition = smallestUncommittedOffsetEntry.getKey();

            Set<Long> processedOffsetSet = getProcessedOffsetSet(topicPartition);

            long smallestProcessedOffset = -1;
            int size = processedOffsetSet.size();
            long offset = smallestUncommittedOffsetEntry.getValue();
            for (int i = 0; i < size && processedOffsetSet.remove(offset); i++, offset++) {
                smallestProcessedOffset = offset;
            }

            if (smallestProcessedOffset > -1) {
                offsetsMapping.put(topicPartition, smallestProcessedOffset);
                smallestUncommittedOffsetEntry.setValue(smallestProcessedOffset + 1);
            }
        }
        return offsetsMapping;
    }

    private boolean hasRecordBeenHandled(TopicPartition recordTopicPartition, long recordOffset) {
        Long smallestUncommittedOffset = this.smallestUncommittedOffsets.get(recordTopicPartition);
        return smallestUncommittedOffset != null && smallestUncommittedOffset > recordOffset;
    }

    @Override
    public <K, V> CompletionStage<Void> handle(final IncomingKafkaRecord<K, V> record) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        context.runOnContext(v -> {
            TopicPartition recordsTopicPartition = getTopicPartition(record);

            if (hasRecordBeenHandled(recordsTopicPartition, record.getOffset())) {
                future.complete(null);
                return;
            }

            getProcessedOffsetSet(recordsTopicPartition).add(record.getOffset());

            if (System.currentTimeMillis() > this.nextCommitTime) {
                resetNextCommitTime();
                Map<TopicPartition, Long> offsetsMapping = getLatestOffsetMapping();

                if (!offsetsMapping.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> offsets = offsetsMapping
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getKey,
                                    e -> new OffsetAndMetadata().setOffset(e.getValue() + 1L)));
                    consumer.getDelegate().commit(offsets, a -> future.complete(null));

                    return;
                }
            }
            future.complete(null);
        });
        return future;

    }

}
