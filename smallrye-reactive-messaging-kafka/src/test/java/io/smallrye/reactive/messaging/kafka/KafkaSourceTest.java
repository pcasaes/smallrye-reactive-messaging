package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;
import javax.enterprise.inject.spi.BeanManager;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.kafka.client.common.TopicPartition;

public class KafkaSourceTest extends KafkaTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSource() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, ic, getConsumerRebalanceListeners());

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSourceWithPartitions() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("partitions", 4);

        kafka.createTopic(topic, 3, 1);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, ic, getConsumerRebalanceListeners());

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(1000, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 1000);

        List<Integer> expected = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        // Because of partitions we cannot enforce the order.
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactlyInAnyOrderElementsOf(expected);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testSourceWithChannelName() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("channel-name", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, ic, getConsumerRebalanceListeners());

        List<KafkaRecord> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(KafkaRecord::getPayload).collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4,
                        5, 6, 7, 8, 9);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testBroadcast() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("broadcast", true);
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnector connector = new KafkaConnector();
        connector.executionHolder = new ExecutionHolder(vertx);
        connector.defaultKafkaConfiguration = UnsatisfiedInstance.instance();
        connector.consumerRebalanceListeners = getConsumerRebalanceListeners();
        connector.init();
        PublisherBuilder<? extends KafkaRecord> builder = (PublisherBuilder<? extends KafkaRecord>) connector
                .getPublisherBuilder(new MapBasedConfig(config));

        List<KafkaRecord> messages1 = new ArrayList<>();
        List<KafkaRecord> messages2 = new ArrayList<>();
        builder.forEach(messages1::add).run();
        builder.forEach(messages2::add).run();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
        await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
        assertThat(messages1.stream().map(KafkaRecord::getPayload).collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4,
                        5, 6, 7, 8, 9);
        assertThat(messages2.stream().map(KafkaRecord::getPayload).collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testBroadcastWithPartitions() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        kafka.createTopic(topic, 2, 1);
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("broadcast", true);
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        config.put("partitions", 2);
        KafkaConnector connector = new KafkaConnector();
        connector.executionHolder = new ExecutionHolder(vertx);
        connector.defaultKafkaConfiguration = UnsatisfiedInstance.instance();
        connector.consumerRebalanceListeners = getConsumerRebalanceListeners();
        connector.init();
        PublisherBuilder<? extends KafkaRecord> builder = (PublisherBuilder<? extends KafkaRecord>) connector
                .getPublisherBuilder(new MapBasedConfig(config));

        List<KafkaRecord> messages1 = new ArrayList<>();
        List<KafkaRecord> messages2 = new ArrayList<>();
        builder.forEach(messages1::add).run();
        builder.forEach(messages2::add).run();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
        await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
        assertThat(messages1.stream().map(KafkaRecord::getPayload).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(0, 1, 2, 3, 4,
                        5, 6, 7, 8, 9);
        assertThat(messages2.stream().map(KafkaRecord::getPayload).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testRetry() throws IOException, InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("retry", true);
        config.put("retry-attempts", 100);
        config.put("retry-max-wait", 30);
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, ic, getConsumerRebalanceListeners());
        List<KafkaRecord> messages1 = new ArrayList<>();
        source.getStream().subscribe().with(m -> messages1.add(m));

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

        restart(5);

        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 20);
        assertThat(messages1.size()).isGreaterThanOrEqualTo(20);
    }

    private Map<String, Object> newCommonConfig() {
        String randomId = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", randomId);
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        return config;
    }

    private MapBasedConfig myKafkaSourceConfig(int partitions, String withConsumerRebalanceListener) {
        String prefix = "mp.messaging.incoming.data.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "group.id", "my-group");
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "enable.auto.commit", "false");
        config.put(prefix + "auto.offset.reset", "earliest");
        config.put(prefix + "topic", "data");
        if (partitions > 0) {
            config.put(prefix + "partitions", Integer.toString(partitions));
            config.put(prefix + "topic", "data-" + partitions);
        }
        if (withConsumerRebalanceListener != null) {
            config.put(prefix + "consumer-rebalance-listener.name", withConsumerRebalanceListener);
        }

        return new MapBasedConfig(config);
    }

    @Test
    public void testABeanConsumingTheKafkaMessagesStartingOnFifthOffsetFromLatest() {
        KafkaUsage usage = new KafkaUsage();
        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean callback = new AtomicBoolean(false);
        new Thread(() -> usage.produceIntegers(10, () -> callback.set(true),
                () -> new ProducerRecord<>("data", counter.getAndIncrement()))).start();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(callback::get);

        ConsumptionBeanWithoutAck bean = deployWithoutAck(
                myKafkaSourceConfig(0, StartFromFifthOffsetFromLatestConsumerRebalanceListener.class.getSimpleName()));
        List<Integer> list = bean.getResults();

        await()
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> list.size() >= 5);

        assertThat(list).containsExactly(6, 7, 8, 9, 10);

        List<KafkaRecord<String, Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            assertThat(m.getTopic()).isEqualTo("data");
            assertThat(m.getTimestamp()).isAfter(Instant.EPOCH);
            assertThat(m.getPartition()).isGreaterThan(-1);
        });

    }

    @Test
    public void testABeanConsumingTheKafkaMessagesWithPartitions() {
        kafka.createTopic("data-2", 2, 1);
        ConsumptionBean bean = deploy(myKafkaSourceConfig(2, ConsumptionConsumerRebalanceListener.class.getSimpleName()));
        KafkaUsage usage = new KafkaUsage();
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("data-2", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<KafkaRecord<String, Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            assertThat(m.getTopic()).isEqualTo("data-2");
            assertThat(m.getTimestamp()).isAfter(Instant.EPOCH);
            assertThat(m.getPartition()).isGreaterThan(-1);
        });

        ConsumptionConsumerRebalanceListener consumptionConsumerRebalanceListener = getConsumptionConsumerRebalanceListener();
        assertThat(consumptionConsumerRebalanceListener.getAssigned().size()).isEqualTo(2);
        for (int i = 0; i < 2; i++) {
            TopicPartition topicPartition = consumptionConsumerRebalanceListener.getAssigned().get(i);
            assertThat(topicPartition).isNotNull();
            assertThat(topicPartition.getTopic()).isEqualTo("data-2");
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInvalidIncomingType() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, ic, getConsumerRebalanceListeners());

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(2, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 2);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1);

        new Thread(() -> usage.produceStrings(1, null, () -> new ProducerRecord<>(topic, "hello"))).start();

        new Thread(() -> usage.produceIntegers(2, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        // no other message received
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testABeanConsumingTheKafkaMessagesWithRawMessage() {
        ConsumptionBeanUsingRawMessage bean = deployRaw(myKafkaSourceConfig(0, null));
        KafkaUsage usage = new KafkaUsage();
        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("data", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<Message<Integer>> messages = bean.getKafkaMessages();
        messages.forEach(m -> {
            IncomingKafkaRecordMetadata<String, Integer> metadata = m.getMetadata(IncomingKafkaRecordMetadata.class)
                    .orElseThrow(() -> new AssertionError("Metadata expected"));
            assertThat(metadata.getTopic()).isEqualTo("data");
            assertThat(metadata.getTimestamp()).isAfter(Instant.EPOCH);
            assertThat(metadata.getPartition()).isGreaterThan(-1);
            assertThat(metadata.getOffset()).isGreaterThan(-1);
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSourceWithEmptyOptionalConfiguration() {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = newCommonConfig();
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("bootstrap.servers", SERVERS);
        config.put("sasl.jaas.config", ""); //optional configuration
        config.put("sasl.mechanism", ""); //optional configuration
        config.put("channel-name", topic);
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(new MapBasedConfig(config));
        KafkaSource<String, Integer> source = new KafkaSource<>(vertx, ic, getConsumerRebalanceListeners());

        List<Message<?>> messages = new ArrayList<>();
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    private BeanManager getBeanManager() {
        if (container == null) {
            Weld weld = baseWeld();
            addConfig(new MapBasedConfig(new HashMap<>()));
            weld.disableDiscovery();
            container = weld.initialize();
        }
        return container.getBeanManager();
    }

    private Instance<KafkaConsumerRebalanceListener> getConsumerRebalanceListeners() {
        return getBeanManager()
                .createInstance()
                .select(KafkaConsumerRebalanceListener.class);
    }

    private ConsumptionConsumerRebalanceListener getConsumptionConsumerRebalanceListener() {
        return getBeanManager()
                .createInstance()
                .select(ConsumptionConsumerRebalanceListener.class)
                .select(NamedLiteral.of(ConsumptionConsumerRebalanceListener.class.getSimpleName()))
                .get();

    }

    private StartFromFifthOffsetFromLatestConsumerRebalanceListener getStartFromFifthOffsetConsumerRebalanceListener() {
        return getBeanManager()
                .createInstance()
                .select(StartFromFifthOffsetFromLatestConsumerRebalanceListener.class)
                .select(NamedLiteral.of(StartFromFifthOffsetFromLatestConsumerRebalanceListener.class.getSimpleName()))
                .get();

    }

    private ConsumptionBean deploy(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ConsumptionConsumerRebalanceListener.class);
        weld.addBeanClass(StartFromFifthOffsetFromLatestConsumerRebalanceListener.class);
        weld.disableDiscovery();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
    }

    private ConsumptionBeanWithoutAck deployWithoutAck(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(ConsumptionBeanWithoutAck.class);
        weld.addBeanClass(ConsumptionConsumerRebalanceListener.class);
        weld.addBeanClass(StartFromFifthOffsetFromLatestConsumerRebalanceListener.class);
        weld.disableDiscovery();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBeanWithoutAck.class).get();
    }

    private ConsumptionBeanUsingRawMessage deployRaw(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(ConsumptionBeanUsingRawMessage.class);
        weld.disableDiscovery();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBeanUsingRawMessage.class).get();
    }

}
