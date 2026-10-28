package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.admin.KafkaConnection;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A page of subscriptions costs a constant number of Admin rounds, whatever its size. */
class KafkaBatchedReadsIT {

    private static final int PAIRS = 12;

    private static KafkaTestCluster kafka;
    private static final Map<String, AtomicInteger> CALLS = new ConcurrentHashMap<>();
    private static KafkaConnection connection;
    private static final List<String> GROUPS = new ArrayList<>();
    private static final List<String> TOPICS = new ArrayList<>();

    @BeforeAll
    static void start() throws Exception {
        kafka = KafkaTestCluster.shared();
        try (Admin admin = Kafka.admin(kafka)) {
            for (int i = 0; i < PAIRS; i++) {
                String topic = Kafka.unique("batch-t");
                String group = Kafka.unique("batch-g");
                TOPICS.add(topic);
                GROUPS.add(group);
                admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1)))
                        .all()
                        .get();
                Map<TopicPartition, OffsetAndMetadata> at = new HashMap<>();
                for (int p = 0; p < 3; p++) {
                    at.put(new TopicPartition(topic, p), new OffsetAndMetadata(0));
                }
                admin.alterConsumerGroupOffsets(group, at).all().get();
            }
            Kafka.eventually(
                    () -> {
                        try {
                            return admin.listOffsets(
                                            Map.of(new TopicPartition(TOPICS.getFirst(), 0), OffsetSpec.latest()))
                                    .all()
                                    .get()
                                    .size();
                        } catch (Exception e) {
                            return 0;
                        }
                    },
                    n -> n == 1);
        }
        DriverContext context = new DriverContext() {
            @Override
            public ConnectOptions options() {
                return ConnectOptions.defaults();
            }

            @Override
            public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
                return factory.get();
            }

            @Override
            public void evict(String key) {}
        };
        KafkaCluster cluster =
                new KafkaCluster(kafka.spec(), CredentialSource.none(), context, KafkaBatchedReadsIT::counting);
        connection = new KafkaConnection(
                new BrokerSystem(KafkaDriver.SYSTEM, "Apache Kafka", "[3.0.0,)", 1, java.util.Set.of(), List.of()),
                cluster,
                context);
    }

    @AfterAll
    static void stop() {
        connection.close();
    }

    /** The Admin client, counting each call made on it by method name. */
    private static Admin counting(Map<String, Object> config) {
        Admin real = Admin.create(config);
        return (Admin) Proxy.newProxyInstance(
                Admin.class.getClassLoader(), new Class<?>[] {Admin.class}, (p, method, args) -> {
                    CALLS.computeIfAbsent(method.getName(), k -> new AtomicInteger())
                            .incrementAndGet();
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static List<ResourceQuery> queries(int n) {
        return java.util.stream.IntStream.range(0, n)
                .mapToObj(i -> ResourceQuery.of(KafkaKinds.GROUP_TOPIC)
                        .where(Map.of("group", GROUPS.get(i), "topic", TOPICS.get(i))))
                .toList();
    }

    private static Map<String, Integer> read(int n) {
        CALLS.clear();
        var result = connection.resources().read(queries(n));
        assertThat(result.failures()).isEmpty();
        List<ResourcePage> pages = result.value();
        assertThat(pages).hasSize(n);
        for (ResourcePage page : pages) {
            assertThat(page.items())
                    .singleElement()
                    .satisfies(r ->
                            assertThat(r.attributes().get(MetricKeys.BACKLOG)).isPresent());
        }
        Map<String, Integer> out = new HashMap<>();
        CALLS.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    @Test
    void aPageOfSubscriptionsCostsTheSameRoundsWhateverItsSize() {
        Map<String, Integer> two = read(2);
        Map<String, Integer> all = read(PAIRS);

        assertThat(all.getOrDefault("describeTopics", 0)).isEqualTo(two.getOrDefault("describeTopics", 0));
        assertThat(all.getOrDefault("listOffsets", 0)).isEqualTo(two.getOrDefault("listOffsets", 0));
        assertThat(all.getOrDefault("listGroups", 0)).isEqualTo(two.getOrDefault("listGroups", 0));
        assertThat(all.getOrDefault("describeConsumerGroups", 0))
                .isEqualTo(two.getOrDefault("describeConsumerGroups", 0));
        assertThat(all.getOrDefault("listConsumerGroupOffsets", 0))
                .isEqualTo(two.getOrDefault("listConsumerGroupOffsets", 0));
        assertThat(all.getOrDefault("listGroups", 0)).isLessThanOrEqualTo(2);
        assertThat(all.getOrDefault("describeTopics", 0)).isLessThanOrEqualTo(2);
        assertThat(all.getOrDefault("listOffsets", 0)).isLessThanOrEqualTo(4);
    }

    @Test
    void aQueryForATopicThatDoesNotExistFailsNoOtherQuery() {
        var queries = new ArrayList<>(queries(2));
        queries.add(ResourceQuery.of(KafkaKinds.GROUP_TOPIC)
                .where(Map.of("group", GROUPS.getFirst(), "topic", Kafka.unique("absent"))));

        var result = connection.resources().read(queries);

        assertThat(result.failures()).isEmpty();
        assertThat(result.value().get(0).items()).hasSize(1);
        assertThat(result.value().get(1).items()).hasSize(1);
        Resource none = result.value().get(2).items().isEmpty()
                ? null
                : result.value().get(2).items().getFirst();
        assertThat(none == null || none.attributes().get(MetricKeys.BACKLOG).isPresent())
                .isTrue();
    }
}
