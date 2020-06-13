package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailStop;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaIgnoreFailure;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;

public class KafkaSource<K, V> {
    private final Multi<IncomingKafkaRecord<K, V>> stream;
    private final KafkaConsumer<K, V> consumer;
    private final KafkaFailureHandler failureHandler;

    public KafkaSource(Vertx vertx,
            KafkaConnectorIncomingConfiguration config,
            Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners) {

        Map<String, String> kafkaConfiguration = new HashMap<>();

        String group = config.getGroupId().orElseGet(() -> {
            String s = UUID.randomUUID().toString();
            log.noGroupId(s);
            return s;
        });

        JsonHelper.asJsonObject(config.config())
                .forEach(e -> kafkaConfiguration.put(e.getKey(), e.getValue().toString()));
        kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, group);

        String servers = config.getBootstrapServers();
        if (!kafkaConfiguration.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            log.configServers(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!kafkaConfiguration.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            log.keyDeserializerOmitted();
            kafkaConfiguration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
        }

        kafkaConfiguration.remove("channel-name");
        kafkaConfiguration.remove("topic");
        kafkaConfiguration.remove("connector");
        kafkaConfiguration.remove("retry");
        kafkaConfiguration.remove("retry-attempts");
        kafkaConfiguration.remove("broadcast");
        kafkaConfiguration.remove("partitions");

        final KafkaConsumer<K, V> kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfiguration);
        final List<Publisher<KafkaConsumerRecord<K, V>>> multis = new ArrayList<>();
        config
                .getConsumerRebalanceListenerName()
                .map(name -> {
                    log.info("Loading KafkaConsumerRebalanceListener " + name);
                    return NamedLiteral.of(name);
                })
                .map(consumerRebalanceListeners::select)
                .map(Instance::get)
                .ifPresent(listener -> multis.add(Multi
                        .createFrom()
                        .publisher(subscriber -> {

                            kafkaConsumer.partitionsAssignedHandler(set -> listener.onPartitionsAssigned(kafkaConsumer, set)
                                    .onFailure().invoke(t -> log.unableToExecuteConsumerReblanceListener(group, t))
                                    .subscribe()
                                    .with(a -> log.executedConsumerRebalanceListener(group)));

                            kafkaConsumer.partitionsRevokedHandler(set -> listener.onPartitionsRevoked(kafkaConsumer, set)
                                    .onFailure().invoke(t -> log.unableToExecuteConsumerReblanceListener(group, t))
                                    .subscribe()
                                    .with(a -> log.executedConsumerRebalanceListener(group)));
                        })));
        this.consumer = kafkaConsumer;

        String topic = config.getTopic().orElseGet(config::getChannel);

        failureHandler = createFailureHandler(config, vertx, kafkaConfiguration);

        Multi<KafkaConsumerRecord<K, V>> consumerMulti = consumer.toMulti()
                .onFailure().invoke(t -> log.unableToReadRecord(topic, t));

        multis.add(consumerMulti);

        Multi<KafkaConsumerRecord<K, V>> multi = multis.size() == 1 ? consumerMulti
                : Multi
                        .createBy()
                        .merging()
                        .streams(multis);

        boolean retry = config.getRetry();
        if (retry) {
            int max = config.getRetryAttempts();
            int maxWait = config.getRetryMaxWait();
            if (max == -1) {
                // always retry
                multi
                        .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(maxWait))
                        .atMost(Long.MAX_VALUE);
            } else {
                multi = multi
                        .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(maxWait))
                        .atMost(max);
            }
        }

        this.stream = multi
                .on().subscribed(s -> {
                    // The Kafka subscription must happen on the subscription.
                    this.consumer.subscribeAndAwait(topic);
                })
                .map(rec -> new IncomingKafkaRecord<>(consumer, rec, failureHandler));
    }

    private KafkaFailureHandler createFailureHandler(KafkaConnectorIncomingConfiguration config, Vertx vertx,
            Map<String, String> kafkaConfiguration) {
        String strategy = config.getFailureStrategy();
        KafkaFailureHandler.Strategy actualStrategy = KafkaFailureHandler.Strategy.from(strategy);
        switch (actualStrategy) {
            case FAIL:
                return new KafkaFailStop(config.getChannel());
            case IGNORE:
                return new KafkaIgnoreFailure(config.getChannel());
            case DEAD_LETTER_QUEUE:
                return KafkaDeadLetterQueue.create(vertx, kafkaConfiguration, config);
            default:
                throw ex.illegalArgumentInvalidStrategy(strategy);
        }

    }

    public Multi<IncomingKafkaRecord<K, V>> getStream() {
        return stream;
    }

    public void closeQuietly() {
        try {
            this.consumer.closeAndAwait();
        } catch (Throwable e) {
            log.exceptionOnClose(e);
        }
    }
}
