package io.smallrye.reactive.messaging.kafka;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(KafkaConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "bootstrap.servers", alias = "kafka.bootstrap.servers", type = "string", defaultValue = "localhost:9092", direction = Direction.INCOMING_AND_OUTGOING, description = "A comma-separated list of host:port to use for establishing the initial connection to the Kafka cluster.")
@ConnectorAttribute(name = "topic", type = "string", direction = Direction.INCOMING_AND_OUTGOING, description = "The consumed / populated Kafka topic. If not set, the channel name is used")
@ConnectorAttribute(name = "key.deserializer", type = "string", direction = Direction.INCOMING, description = "The deserializer classname used to deserialize the record's key", defaultValue = "org.apache.kafka.common.serialization.StringDeserializer")
@ConnectorAttribute(name = "value.deserializer", type = "string", direction = Direction.INCOMING, description = "The deserializer classname used to deserialize the record's value", mandatory = true)
@ConnectorAttribute(name = "fetch.min.bytes", type = "int", direction = Direction.INCOMING, description = "The minimum amount of data the server should return for a fetch request. The default setting of 1 byte means that fetch requests are answered as soon as a single byte of data is available or the fetch request times out waiting for data to arrive.", defaultValue = "1")
@ConnectorAttribute(name = "group.id", type = "string", direction = Direction.INCOMING, description = "A unique string that identifies the consumer group the application belongs to. If not set, a unique, generated id is used")
@ConnectorAttribute(name = "retry", type = "boolean", direction = Direction.INCOMING, description = "Whether or not the connection to the broker is re-attempted in case of failure", defaultValue = "true")
@ConnectorAttribute(name = "retry-attempts", type = "int", direction = Direction.INCOMING, description = "The maximum number of reconnection before failing. -1 means infinite retry", defaultValue = "-1")
@ConnectorAttribute(name = "retry-max-wait", type = "int", direction = Direction.INCOMING, description = "The max delay (in seconds) between 2 reconnects", defaultValue = "30")
@ConnectorAttribute(name = "broadcast", type = "boolean", direction = Direction.INCOMING, description = "Whether the Kafka records should be dispatched to multiple consumer", defaultValue = "false")
@ConnectorAttribute(name = "auto.offset.reset", type = "string", direction = Direction.INCOMING, description = "What to do when there is no initial offset in Kafka.Accepted values are earliest, latest and none", defaultValue = "latest")
@ConnectorAttribute(name = "failure-strategy", type = "string", direction = Direction.INCOMING, description = "Specify the failure strategy to apply when a message produced from a record is nacked. Values can be `fail` (default), `ignore`, or `dead-letter-queue`", defaultValue = "fail")
@ConnectorAttribute(name = "dead-letter-queue.topic", type = "string", direction = Direction.INCOMING, description = "When the `failure-strategy` is set to `dead-letter-queue` indicates on which topic the record is sent. Defaults is `dead-letter-topic-$channel`")
@ConnectorAttribute(name = "dead-letter-queue.key.serializer", type = "string", direction = Direction.INCOMING, description = "When the `failure-strategy` is set to `dead-letter-queue` indicates the key serializer to use. If not set the serializer associated to the key deserializer is used")
@ConnectorAttribute(name = "dead-letter-queue.value.serializer", type = "string", direction = Direction.INCOMING, description = "When the `failure-strategy` is set to `dead-letter-queue` indicates the value serializer to use. If not set the serializer associated to the value deserializer is used")
@ConnectorAttribute(name = "partitions", type = "int", direction = Direction.INCOMING, description = "The number of partitions to be consumed concurrently. The connector creates the specified amount of Kafka consumers. It should match the number of partition of the targetted topic", defaultValue = "1")
@ConnectorAttribute(name = "key.serializer", type = "string", direction = Direction.OUTGOING, description = "The serializer classname used to serialize the record's key", defaultValue = "org.apache.kafka.common.serialization.StringSerializer")
@ConnectorAttribute(name = "value.serializer", type = "string", direction = Direction.OUTGOING, description = "The serializer classname used to serialize the payload", mandatory = true)
@ConnectorAttribute(name = "acks", type = "string", direction = Direction.OUTGOING, description = "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the durability of records that are sent. Accepted values are: 0, 1, all", defaultValue = "1")
@ConnectorAttribute(name = "buffer.memory", type = "long", direction = Direction.OUTGOING, description = "The total bytes of memory the producer can use to buffer records waiting to be sent to the server.", defaultValue = "33554432")
@ConnectorAttribute(name = "retries", type = "long", direction = Direction.OUTGOING, description = "Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error.", defaultValue = "2147483647")
@ConnectorAttribute(name = "key", type = "string", direction = Direction.OUTGOING, description = "A key to used when writing the record")
@ConnectorAttribute(name = "partition", type = "int", direction = Direction.OUTGOING, description = "The target partition id. -1 to let the client determine the partition", defaultValue = "-1")
@ConnectorAttribute(name = "waitForWriteCompletion", type = "boolean", direction = Direction.OUTGOING, description = "Whether the client waits for Kafka to acknowledge the written record before acknowledging the message", defaultValue = "true")
@ConnectorAttribute(name = "max-inflight-messages", type = "int", direction = Direction.OUTGOING, description = "The maximum number of messages to be written to Kafka concurrently - The default value is the value from the `max.in.flight.requests.per.connection` Kafka property. It configures the maximum number of unacknowledged requests the client before blocking. Note that if this setting is set to be greater than 1 and there are failed sends, there is a risk of message re-ordering due to retries.", defaultValue = "5")
@ConnectorAttribute(name = "consumer-rebalance-listener.name", type = "string", direction = Direction.INCOMING, description = "The name set in `javax.inject.Named` of a bean that implements `io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener`. If set the listener will be applied to the consumer.")
public class KafkaConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    public static final String CONNECTOR_NAME = "smallrye-kafka";

    @Inject
    ExecutionHolder executionHolder;

    @Inject
    Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners;

    private final List<KafkaSource<?, ?>> sources = new CopyOnWriteArrayList<>();
    private final List<KafkaSink> sinks = new CopyOnWriteArrayList<>();

    @Inject
    @Named("default-kafka-broker")
    Instance<Map<String, Object>> defaultKafkaConfiguration;

    private Vertx vertx;

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        sources.forEach(KafkaSource::closeQuietly);
        sinks.forEach(KafkaSink::closeQuietly);
    }

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        Config c = config;
        if (!defaultKafkaConfiguration.isUnsatisfied()) {
            c = merge(config, defaultKafkaConfiguration.get());
        }
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(c);
        int partitions = ic.getPartitions();
        if (partitions <= 0) {
            throw new IllegalArgumentException("`partitions` must be greater than 0");
        }

        if (partitions == 1) {
            KafkaSource<Object, Object> source = new KafkaSource<>(vertx, ic, consumerRebalanceListeners);
            sources.add(source);

            boolean broadcast = ic.getBroadcast();
            if (broadcast) {
                return ReactiveStreams.fromPublisher(source.getStream().broadcast().toAllSubscribers());
            } else {
                return ReactiveStreams.fromPublisher(source.getStream());
            }
        }

        // create an instance of source per partitions.
        List<Publisher<IncomingKafkaRecord<Object, Object>>> streams = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            KafkaSource<Object, Object> source = new KafkaSource<>(vertx, ic, consumerRebalanceListeners);
            sources.add(source);
            streams.add(source.getStream());
        }

        Multi<IncomingKafkaRecord<Object, Object>> multi = Multi.createBy().merging().streams(streams);
        boolean broadcast = ic.getBroadcast();
        if (broadcast) {
            return ReactiveStreams.fromPublisher(multi.broadcast().toAllSubscribers());
        } else {
            return ReactiveStreams.fromPublisher(multi);
        }
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        Config c = config;
        if (!defaultKafkaConfiguration.isUnsatisfied()) {
            c = merge(config, defaultKafkaConfiguration.get());
        }
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(c);
        KafkaSink sink = new KafkaSink(vertx, oc);
        sinks.add(sink);
        return sink.getSink();
    }

    private Config merge(Config passedCfg, Map<String, Object> defaultKafkaCfg) {
        log.mergingConfigWith(defaultKafkaCfg);

        return new Config() {
            @SuppressWarnings("unchecked")
            @Override
            public <T> T getValue(String propertyName, Class<T> propertyType) {
                T t = (T) defaultKafkaCfg.get(propertyName);
                if (t == null) {
                    return passedCfg.getValue(propertyName, propertyType);
                }
                return t;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
                T def = (T) defaultKafkaCfg.get(propertyName);
                if (def != null) {
                    return Optional.of(def);
                }
                return passedCfg.getOptionalValue(propertyName, propertyType);
            }

            @Override
            public Iterable<String> getPropertyNames() {
                Iterable<String> names = passedCfg.getPropertyNames();
                Set<String> result = new HashSet<>();
                names.forEach(result::add);
                result.addAll(defaultKafkaCfg.keySet());
                return result;
            }

            @Override
            public Iterable<ConfigSource> getConfigSources() {
                return passedCfg.getConfigSources();
            }
        };
    }
}
