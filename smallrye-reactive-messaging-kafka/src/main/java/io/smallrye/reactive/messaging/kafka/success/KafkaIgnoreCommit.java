package io.smallrye.reactive.messaging.kafka.success;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;

public class KafkaIgnoreCommit implements KafkaCommitHandler {

    @Override
    public <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record) {
        return CompletableFuture.completedFuture(null);
    }
}
