package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.Ordering;
import io.github.sudoitir.openbroker.model.PlacementRole;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Verb;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The kinds, and listing and changing topics, partitions, groups and ACLs. */
class KafkaResourcesIT {

    private static KafkaTestCluster kafka;
    private static BrokerDrivers drivers;
    private static ClusterConnection connection;

    @BeforeAll
    static void start() {
        kafka = KafkaTestCluster.shared();
        drivers = BrokerDrivers.load();
        connection = Kafka.connect(drivers, kafka);
    }

    @AfterAll
    static void stop() {
        connection.close();
        drivers.close();
    }

    private static List<Resource> list(ResourceQuery query) {
        return connection.resources().read(query).orThrow().items();
    }

    private static ResourceId topic(String name) {
        return new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(name));
    }

    private static void create(ResourceId id, Attributes attributes) {
        connection
                .operations()
                .execute(id, new Operation.Create(attributes), Scope.whole())
                .orThrow();
    }

    @Test
    void everyKindCarriesKafkasOwnLabel() {
        Map<String, String> labels = new TreeMap<>();
        connection
                .resources()
                .kinds()
                .forEach(k -> labels.put(k.id().value(), k.label().singular()));

        assertThat(labels)
                .containsEntry("kafka.topic", "Topic")
                .containsEntry("kafka.partition", "Partition")
                .containsEntry("kafka.group", "Group")
                .containsEntry("kafka.group-topic", "Group on topic")
                .containsEntry("kafka.acl", "ACL")
                .containsEntry("kafka.transactional-id", "Transactional ID");
    }

    @Test
    void theShareGroupKindIsDeclaredOnlyWhereShareGroupsRunInProduction() {
        boolean declared =
                connection.resources().kinds().stream().anyMatch(k -> k.id().equals(KafkaKinds.SHARE_GROUP_TOPIC));

        assertThat(declared).isEqualTo(!KafkaTestCluster.image().startsWith("apache/kafka:4.0"));
    }

    @Test
    void kindsMapToTheResourceClassesOfTheModel() {
        Map<String, ResourceClass> classes = new TreeMap<>();
        connection.resources().kinds().forEach(k -> classes.put(k.id().value(), k.resourceClass()));

        assertThat(classes)
                .containsEntry("kafka.topic", ResourceClass.DESTINATION)
                .containsEntry("kafka.partition", ResourceClass.PARTITION)
                .containsEntry("kafka.group", ResourceClass.CONSUMER_GROUP)
                .containsEntry("kafka.group-topic", ResourceClass.SUBSCRIPTION)
                .containsEntry("kafka.acl", ResourceClass.ACCESS_RULE)
                .containsEntry("kafka.transactional-id", ResourceClass.TRANSACTION);
    }

    @Test
    void perMessageMoveCopyAndDeleteAreDeclaredOnNoKind() {
        assertThat(connection.resources().kinds())
                .allSatisfy(k -> assertThat(k.verbs())
                        .doesNotContain(Verb.MOVE_MESSAGES, Verb.COPY_MESSAGES, Verb.DELETE_MESSAGES));
    }

    @Test
    void aTopicIsCreatedListedChangedAndDeleted() {
        String name = Kafka.unique("topic");
        ResourceId id = topic(name);
        create(
                id,
                Attributes.builder()
                        .putRaw(KafkaKinds.PARTITIONS, 3L)
                        .putRaw(KafkaKinds.CONFIG_PREFIX + "retention.ms", "86400000")
                        .build());

        Resource created = Kafka.eventually(
                () -> list(ResourceQuery.of(KafkaKinds.TOPIC).named(name)).getFirst(),
                r -> r.attributes()
                        .raw(KafkaKinds.CONFIG_PREFIX + "retention.ms")
                        .isPresent());
        assertThat(created.id()).isEqualTo(id);
        assertThat(created.attributes().raw(KafkaKinds.PARTITIONS)).contains(3L);
        assertThat(created.attributes().raw(KafkaKinds.REPLICATION_FACTOR)).contains(3L);
        assertThat(created.attributes().raw(KafkaKinds.CONFIG_PREFIX + "retention.ms"))
                .contains("86400000");

        connection
                .operations()
                .execute(
                        id,
                        new Operation.Update(Attributes.builder()
                                .putRaw(KafkaKinds.PARTITIONS, 5L)
                                .putRaw(KafkaKinds.CONFIG_PREFIX + "retention.ms", "3600000")
                                .build()),
                        Scope.whole())
                .orThrow();
        Resource changed = Kafka.eventually(
                () -> list(ResourceQuery.of(KafkaKinds.TOPIC).named(name)).getFirst(),
                r -> r.attributes()
                        .raw(KafkaKinds.CONFIG_PREFIX + "retention.ms")
                        .equals(Optional.of("3600000")));
        assertThat(changed.attributes().raw(KafkaKinds.PARTITIONS)).contains(5L);
        assertThat(changed.attributes().raw(KafkaKinds.CONFIG_PREFIX + "retention.ms"))
                .contains("3600000");

        connection.operations().execute(id, Operation.delete(), Scope.whole()).orThrow();
        assertThat(Kafka.eventually(
                        () -> list(ResourceQuery.of(KafkaKinds.TOPIC).named(name)), List::isEmpty))
                .isEmpty();
    }

    @Test
    void creatingATopicTwiceIsAlreadyInThatState() {
        ResourceId id = topic(Kafka.unique("again"));
        create(id, Attributes.empty());

        OperationResult again = connection
                .operations()
                .execute(id, new Operation.Create(Attributes.empty()), Scope.whole())
                .orThrow();

        assertThat(again.alreadyInState()).isTrue();
    }

    @Test
    void aTopicsPartitionsCanOnlyBeIncreased() {
        String name = Kafka.unique("shrink");
        ResourceId id = topic(name);
        create(id, Attributes.builder().putRaw(KafkaKinds.PARTITIONS, 3L).build());

        Partial<OperationResult> result = connection
                .operations()
                .execute(
                        id,
                        new Operation.Update(Attributes.builder()
                                .putRaw(KafkaKinds.PARTITIONS, 2L)
                                .build()),
                        Scope.whole());

        assertThat(result.failures().values()).singleElement().satisfies(e -> {
            assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
            assertThat(e.reason()).isEqualTo(ErrorReasons.IMMUTABLE_FIELD);
        });
    }

    @Test
    void aPreviewOfAnUpdateShowsWhatChanges() {
        String name = Kafka.unique("preview");
        ResourceId id = topic(name);
        create(
                id,
                Attributes.builder()
                        .putRaw(KafkaKinds.CONFIG_PREFIX + "retention.ms", "1000")
                        .build());

        var preview = connection
                .operations()
                .preview(
                        id,
                        new Operation.Update(Attributes.builder()
                                .putRaw(KafkaKinds.CONFIG_PREFIX + "retention.ms", "2000")
                                .build()),
                        Scope.whole());

        assertThat(preview.diff()).singleElement().satisfies(change -> {
            assertThat(change.key()).isEqualTo(KafkaKinds.CONFIG_PREFIX + "retention.ms");
            assertThat(change.before()).contains("1000");
            assertThat(change.after()).contains("2000");
        });
        assertThat(preview.etag()).isNotBlank();
    }

    @Test
    void partitionsListLeadersReplicasAndOffsets() {
        String name = Kafka.unique("parts");
        create(
                topic(name),
                Attributes.builder().putRaw(KafkaKinds.PARTITIONS, 4L).build());

        List<Resource> partitions = list(ResourceQuery.of(KafkaKinds.PARTITION).where(Map.of("topic", name)));

        assertThat(partitions).hasSize(4);
        assertThat(partitions).allSatisfy(p -> {
            assertThat(p.id().path().segments().getFirst()).isEqualTo(name);
            assertThat(p.attributes().raw(KafkaKinds.LEADER)).isPresent();
            assertThat(p.attributes().raw(KafkaKinds.LOG_START_OFFSET)).contains(0L);
            assertThat(p.attributes().raw(KafkaKinds.LOG_END_OFFSET)).contains(0L);
            assertThat(p.attributes().raw(KafkaKinds.SIZE_BYTES)).isPresent();
            assertThat(p.placements()).hasSize(3);
            assertThat(p.placements().stream().filter(x -> x.role() == PlacementRole.LEADER))
                    .hasSize(1);
            assertThat(p.placements()).allSatisfy(x -> assertThat(x.inSync()).contains(true));
        });
    }

    @Test
    void aProjectionSkipsTheReadsItDoesNotNeed() {
        String name = Kafka.unique("proj");
        create(topic(name), Attributes.empty());

        List<Resource> partitions = list(ResourceQuery.of(KafkaKinds.PARTITION)
                .where(Map.of("topic", name))
                .project(Set.of(KafkaKinds.LEADER)));

        assertThat(partitions.getFirst().attributes().asMap()).containsOnlyKeys(KafkaKinds.LEADER);
    }

    @Test
    void aListingIsPagedByAContinueToken() {
        String prefix = Kafka.unique("page");
        for (int i = 0; i < 5; i++) {
            create(topic(prefix + "-" + i), Attributes.empty());
        }
        ResourceQuery query =
                ResourceQuery.of(KafkaKinds.TOPIC).named(prefix + "-*").limit(2);

        ResourcePage first = connection.resources().read(query).orThrow();
        ResourcePage second = connection
                .resources()
                .read(query.continueFrom(first.continueToken().orElseThrow()))
                .orThrow();
        ResourcePage third = connection
                .resources()
                .read(query.continueFrom(second.continueToken().orElseThrow()))
                .orThrow();

        assertThat(first.items()).hasSize(2);
        assertThat(second.items()).hasSize(2);
        assertThat(third.items()).hasSize(1);
        assertThat(third.continueToken()).isEmpty();
    }

    @Test
    void anUnknownSelectorOrOrderingIsRefused() {
        assertThatThrownBy(() -> connection
                        .resources()
                        .read(ResourceQuery.of(KafkaKinds.TOPIC).where(Map.of("colour", "red")))
                        .orThrow())
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
        assertThatThrownBy(() -> connection
                        .resources()
                        .read(ResourceQuery.of(KafkaKinds.TOPIC).orderBy(List.of(Ordering.asc("colour"))))
                        .orThrow())
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }

    @Test
    void anAclIsCreatedListedAndDeleted() {
        String name = Kafka.unique("acl-topic");
        ResourceId id = new ResourceId(
                KafkaKinds.ACL, ResourcePath.of("User:alice", "*", "read", "allow", "topic", "literal", name));

        create(id, Attributes.empty());
        List<Resource> found =
                Kafka.eventually(() -> list(ResourceQuery.of(KafkaKinds.ACL).named(name)), l -> l.size() == 1);
        assertThat(found).singleElement().satisfies(r -> {
            assertThat(r.id()).isEqualTo(id);
            assertThat(r.attributes().raw(KafkaKinds.PRINCIPAL)).contains("User:alice");
            assertThat(r.attributes().raw(KafkaKinds.OPERATION)).contains("read");
        });
        // Making it again is not an error, whether or not this read of the rule is up to date.
        connection
                .operations()
                .execute(id, new Operation.Create(Attributes.empty()), Scope.whole())
                .orThrow();

        connection.operations().execute(id, Operation.delete(), Scope.whole()).orThrow();
        assertThat(Kafka.eventually(() -> list(ResourceQuery.of(KafkaKinds.ACL).named(name)), List::isEmpty))
                .isEmpty();
    }

    @Test
    void aKindWithNoSuchVerbIsRefused() {
        Kind acl = connection.resources().kinds().stream()
                .filter(k -> k.id().equals(KafkaKinds.ACL))
                .findFirst()
                .orElseThrow();
        ResourceId id = new ResourceId(
                KafkaKinds.ACL, ResourcePath.of("User:bob", "*", "read", "allow", "topic", "literal", "x"));

        assertThat(acl.supports(Verb.PURGE)).isFalse();
        assertThat(connection
                        .operations()
                        .execute(id, Operation.purge(), Scope.whole())
                        .failures()
                        .values())
                .singleElement()
                .satisfies(e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
    }
}
