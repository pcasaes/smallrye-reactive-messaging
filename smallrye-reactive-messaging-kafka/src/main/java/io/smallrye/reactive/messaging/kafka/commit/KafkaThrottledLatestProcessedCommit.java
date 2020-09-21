package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

/**
 * Will keep track of received messages and commit to the next offset after the latest
 * ACKed message in sequence. Will only commit every 5 seconds.
 * <p>
 * This strategy mimics the behavior of the kafka consumer when `enable.auto.commit`
 * is `true`.
 * <p>
 * If too many received messages are not ACKed then the KafkaSource will be marked
 * as unhealthy. "Too many" is defined as a power of two value greater than or equal
 * to `max.poll.records` times 2.
 * <p>
 * This strategy guarantees at-least-once delivery even if the channel performs
 * asynchronous processing.
 * <p>
 * To use set `commit-strategy` to `throttled`.
 */
public class KafkaThrottledLatestProcessedCommit implements KafkaCommitHandler {

    private static final Map<String, Map<Integer, TopicPartition>> TOPIC_PARTITIONS_CACHE = new ConcurrentHashMap<>();

    private final Map<TopicPartition, OffsetStore> offsetStores = new HashMap<>();

    private final KafkaConsumer<?, ?> consumer;
    private final KafkaSource<?, ?> source;
    private final int unprocessedRecordMaxAge;
    private final int autoCommitInterval;

    private volatile Context context;

    private Long timerId = null;

    private KafkaThrottledLatestProcessedCommit(KafkaConsumer<?, ?> consumer,
            KafkaSource<?, ?> source,
            int unprocessedRecordMaxAge,
            int autoCommitInterval) {
        this.consumer = consumer;
        this.source = source;
        this.unprocessedRecordMaxAge = unprocessedRecordMaxAge;
        this.autoCommitInterval = autoCommitInterval;
    }

    public static void clearCache() {
        TOPIC_PARTITIONS_CACHE.clear();
    }

    public static KafkaThrottledLatestProcessedCommit create(KafkaConsumer<?, ?> consumer,
            Map<String, String> config,
            KafkaSource<?, ?> source) {

        int unprocessedRecordMaxAge = Integer.parseInt(config.getOrDefault("throttled.unprocessed-record-max-age", "60000"));
        int autoCommitInterval = Integer.parseInt(config.getOrDefault(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000"));
        log.settingCommitInterval(config.get(ConsumerConfig.GROUP_ID_CONFIG), autoCommitInterval);
        return new KafkaThrottledLatestProcessedCommit(consumer, source, unprocessedRecordMaxAge, autoCommitInterval);

    }

    private <K, V> TopicPartition getTopicPartition(IncomingKafkaRecord<K, V> record) {
        return TOPIC_PARTITIONS_CACHE
                .computeIfAbsent(record.getTopic(), topic -> new ConcurrentHashMap<>())
                .computeIfAbsent(record.getPartition(), partition -> new TopicPartition(record.getTopic(), partition));
    }

    private OffsetStore getOffsetStore(TopicPartition topicPartition) {
        return offsetStores
                .computeIfAbsent(topicPartition, k -> new OffsetStore(k, unprocessedRecordMaxAge));
    }

    @Override
    public void partitionsAssigned(Context context, Set<TopicPartition> partitions) {
        this.context = context;

        offsetStores.clear();

        if (partitions.isEmpty()) {
            if (timerId != null) {
                context.owner().cancelTimer(timerId);
                timerId = null;
            }
        } else {
            if (timerId == null) {
                timerId = context
                        .owner()
                        .setPeriodic(autoCommitInterval, this::flushAndCheckHealth);
            }
        }

    }

    @Override
    public <K, V> IncomingKafkaRecord<K, V> received(IncomingKafkaRecord<K, V> record) {
        TopicPartition recordsTopicPartition = getTopicPartition(record);
        getOffsetStore(recordsTopicPartition).received(record.getOffset());

        return record;
    }

    private Map<TopicPartition, Long> clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping() {
        Map<TopicPartition, Long> offsetsMapping = new HashMap<>();

        offsetStores
                .entrySet()
                .forEach(offsetStoreEntry -> {
                    TopicPartition topicPartition = offsetStoreEntry.getKey();
                    offsetStoreEntry
                            .getValue()
                            .clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset()
                            .ifPresent(offset -> offsetsMapping.put(topicPartition, offset));
                });

        return offsetsMapping;
    }

    @Override
    public <K, V> CompletionStage<Void> handle(final IncomingKafkaRecord<K, V> record) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        context.runOnContext(v -> {
            offsetStores
                    .get(getTopicPartition(record))
                    .processed(record.getOffset());
            future.complete(null);
        });
        return future;

    }

    private void flushAndCheckHealth(long timerId) {
        Map<TopicPartition, Long> offsetsMapping = clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping();

        if (!offsetsMapping.isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsMapping
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> new OffsetAndMetadata().setOffset(e.getValue() + 1L)));
            consumer.getDelegate().commit(offsets);
        }

        offsetStores
                .values()
                .forEach(offsetStoreEntry -> {
                    if (offsetStoreEntry.hasTooManyMessagesWithoutAck()) {
                        this.source.reportFailure(new TooManyMessagegsWithoutAckingException());
                    }
                });
    }

    private static class OffsetReceivedAt {
        private final long offset;
        private final long receivedAt;

        private OffsetReceivedAt(long offset, long receivedAt) {
            this.offset = offset;
            this.receivedAt = receivedAt;
        }

        static OffsetReceivedAt received(long offset) {
            return new OffsetReceivedAt(offset, System.currentTimeMillis());
        }

        public long getOffset() {
            return offset;
        }

        public long getReceivedAt() {
            return receivedAt;
        }
    }

    private static class OffsetStore {

        private final TopicPartition topicPartition;
        private final Queue<OffsetReceivedAt> receivedOffsets = new LinkedList<>();
        private final Set<Long> processedOffsets = new HashSet<>();
        private final int unprocessedRecordMaxAge;
        private long unProcessedTotal = 0L;

        OffsetStore(TopicPartition topicPartition, int unprocessedRecordMaxAge) {
            this.topicPartition = topicPartition;
            this.unprocessedRecordMaxAge = unprocessedRecordMaxAge;
        }

        void received(long offset) {
            this.receivedOffsets.offer(OffsetReceivedAt.received(offset));
            unProcessedTotal++;
        }

        void processed(long offset) {
            if (!this.receivedOffsets.isEmpty() && this.receivedOffsets.peek().getOffset() <= offset) {
                processedOffsets.add(offset);
            }
        }

        OptionalLong clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset() {
            if (!processedOffsets.isEmpty()) {
                long largestSequentialProcessedOffset = -1;
                while (!receivedOffsets.isEmpty()) {
                    if (!processedOffsets.remove(receivedOffsets.peek().getOffset())) {
                        break;
                    }
                    unProcessedTotal--;
                    largestSequentialProcessedOffset = receivedOffsets.poll().getOffset();
                }

                if (largestSequentialProcessedOffset > -1) {
                    return OptionalLong.of(largestSequentialProcessedOffset);
                }
            }
            return OptionalLong.empty();
        }

        boolean hasTooManyMessagesWithoutAck() {
            if (receivedOffsets.isEmpty()) {
                return false;
            }
            if (System.currentTimeMillis() - receivedOffsets.peek().getReceivedAt() > unprocessedRecordMaxAge) {
                log.receivedTooManyMessagesWithoutAcking(topicPartition.toString(), unProcessedTotal);
                return true;
            }
            return false;
        }
    }

    public static class TooManyMessagegsWithoutAckingException extends Exception {
        public TooManyMessagegsWithoutAckingException() {
            super("Too Many Messages without Acking");
        }
    }

}
