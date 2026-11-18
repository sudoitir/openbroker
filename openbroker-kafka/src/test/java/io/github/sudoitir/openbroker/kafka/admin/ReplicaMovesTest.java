package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.operation.Election;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

/** Refusals that send nothing, and the decision that puts a throttle back. */
class ReplicaMovesTest {

    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);
    private static final Instant NOW = Instant.parse("2026-11-18T12:00:00Z");

    @Test
    void uncleanAndEmptyElectionsAreRefusedBeforeAnyCall() {
        assertThatThrownBy(() -> ReplicaMoves.checkElection(Election.UNCLEAN, Set.of(ORDERS)))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertThat(e.reason()).isEqualTo(ErrorReasons.PRECONDITION_FAILED);
                    assertThat(e.getMessage()).containsIgnoringCase("unclean");
                });
        assertThatThrownBy(() -> ReplicaMoves.checkElection(Election.PREFERRED, Set.of()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertThat(e.getMessage()).contains("empty");
                });
        assertThatThrownBy(() -> ReplicaMoves.checkElection(Election.PREFERRED, null))
                .isInstanceOf(BrokerException.class);
    }

    @Test
    void aMissingSizeIsUnavailableAndNeverZero() {
        ReplicaLogs.Read missing = new ReplicaLogs.Read(Map.of(), "broker 1: no reply");

        Count bytes = ReplicaMoves.bytesToCopy(List.of(1), List.of(1, 2), missing, ORDERS, new Node(1, "one", 9092));

        assertThat(bytes).isInstanceOf(Count.Unavailable.class);
        assertThat(((Count.Unavailable) bytes).reason()).contains("broker 1").doesNotContain("0");
    }

    @Test
    void bytesToCopyAreTheLeaderSizeOfEachAddedReplica() {
        ReplicaLogs.Broker broker = new ReplicaLogs.Broker(Map.of(ORDERS, new ReplicaInfo(100, 40, false)), Map.of());
        ReplicaLogs.Read read = new ReplicaLogs.Read(Map.of(1, broker), "");

        Count bytes = ReplicaMoves.bytesToCopy(List.of(1), List.of(1, 2), read, ORDERS, new Node(1, "one", 9092));

        assertThat(bytes).isEqualTo(Count.estimate(100));
        assertThat(ReplicaMoves.duration(bytes, 10)).contains("10s").contains("estimate");
        assertThat(ReplicaMoves.duration(Count.unavailable("no size"), 10))
                .contains("unavailable")
                .contains("no size");
    }

    @Test
    void aPreExistingThrottleIsNotAddedAndAnAbsentOneIs() {
        ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "1");
        ReplicaMoves.ThrottleKey existing =
                new ReplicaMoves.ThrottleKey(broker, ReplicaMoves.LEADER_RATE, Optional.of("8192"), "8192");
        ReplicaMoves.ThrottleKey created =
                new ReplicaMoves.ThrottleKey(broker, ReplicaMoves.FOLLOWER_RATE, Optional.empty(), "8192");

        assertThat(ReplicaMoves.change(existing)).isEmpty();
        assertThat(ReplicaMoves.change(created))
                .hasValueSatisfying(write -> assertThat(write.added()).isTrue());
        Attributes attributes = ReplicaMoves.throttleAttributes(List.of(existing, created));
        assertThat(attributes.raw(KafkaKinds.THROTTLE_PREVIOUS))
                .contains(Map.of("broker.1." + ReplicaMoves.LEADER_RATE, "8192"));
        assertThat(attributes.raw(KafkaKinds.THROTTLE_ADDED))
                .contains(List.of("broker.1." + ReplicaMoves.FOLLOWER_RATE));
    }

    @Test
    void aFailedAlterRestoresOnlyKeysThisCallChanged() {
        ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "1");
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, "orders");
        List<ReplicaMoves.ThrottleWrite> changed = List.of(
                new ReplicaMoves.ThrottleWrite(broker, ReplicaMoves.LEADER_RATE, "100", Optional.of("40"), false),
                new ReplicaMoves.ThrottleWrite(topic, ReplicaMoves.FOLLOWER_REPLICAS, "0:2", Optional.empty(), true));

        Map<ConfigResource, Collection<AlterConfigOp>> ops = ReplicaMoves.restore(changed);

        assertThat(ops).containsOnlyKeys(broker, topic);
        assertThat(ops.get(broker)).singleElement().satisfies(op -> {
            assertThat(op.opType()).isEqualTo(AlterConfigOp.OpType.SET);
            assertThat(op.configEntry().value()).isEqualTo("40");
        });
        assertThat(ops.get(topic)).singleElement().satisfies(op -> {
            assertThat(op.opType()).isEqualTo(AlterConfigOp.OpType.DELETE);
            assertThat(op.configEntry().name()).isEqualTo(ReplicaMoves.FOLLOWER_REPLICAS);
        });
        assertThat(ops.keySet()).noneMatch(resource -> resource.name().equals("untouched"));
    }

    @Test
    void aRefusedReassignmentIsNotAskedAgain() {
        Admin admin = mock(Admin.class);
        KafkaCluster cluster = cluster();
        cluster.decide(
                KafkaAccess.REASSIGNMENT, Assessment.unavailable("the broker rejected reassignment", Optional.empty()));
        Context context = new Context(cluster, null, admin, await(), NOW, null);

        assertThatThrownBy(() -> ReplicaMoves.require(context, KafkaAccess.REASSIGNMENT))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
                    assertThat(e.reason()).isEqualTo(ErrorReasons.UNSUPPORTED_VERSION);
                    assertThat(e.getMessage()).contains("rejected reassignment");
                });
        assertThat(ReplicaMoves.reassignments(context, Set.of(ORDERS))).isEmpty();
        verify(admin, never()).listPartitionReassignments(anySet(), any());
    }

    @Test
    void inProgressReassignmentsAreListedForTheNamedPartitions() {
        Admin admin = mock(Admin.class);
        PartitionReassignment reassignment = new PartitionReassignment(List.of(1, 2), List.of(2), List.of());
        ListPartitionReassignmentsResult result = mock(ListPartitionReassignmentsResult.class);
        when(result.reassignments()).thenReturn(KafkaFuture.completedFuture(Map.of(ORDERS, reassignment)));
        when(admin.listPartitionReassignments(anySet(), any(ListPartitionReassignmentsOptions.class)))
                .thenReturn(result);
        Context context = new Context(cluster(), null, admin, await(), NOW, null);

        assertThat(ReplicaMoves.reassignments(context, Set.of(ORDERS))).containsEntry(ORDERS, reassignment);
        assertThat(ReplicaMoves.reassignmentId(reassignment))
                .contains("adding=")
                .contains("2");
        assertThat(ReplicaMoves.reassignments(context, Set.of())).isEmpty();
        verify(admin).listPartitionReassignments(anySet(), any(ListPartitionReassignmentsOptions.class));
    }

    @Test
    void aThrottleThatIsNotAPositiveLongIsRefused() {
        assertThatThrownBy(() -> ReplicaMoves.requiredRate(Attributes.empty()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION));
        Attributes zero = Attributes.builder().putRaw(ReplicaMoves.THROTTLE, 0L).build();
        assertThatThrownBy(() -> ReplicaMoves.requiredRate(zero)).isInstanceOf(BrokerException.class);
        Attributes text =
                Attributes.builder().putRaw(ReplicaMoves.THROTTLE, "fast").build();
        assertThatThrownBy(() -> ReplicaMoves.requiredRate(text)).isInstanceOf(BrokerException.class);
        Attributes rate =
                Attributes.builder().putRaw(ReplicaMoves.THROTTLE, "1024").build();
        assertThat(ReplicaMoves.requiredRate(rate)).isEqualTo(1024L);
    }

    @Test
    void durationNamesAnEstimateAndAnUnknownSize() {
        assertThat(ReplicaMoves.duration(Count.estimate(1000), 100))
                .contains("10s")
                .contains("estimate");
        assertThat(ReplicaMoves.duration(Count.unavailable("no leader"), 100)).contains("no leader");
    }

    @Test
    void anExactCountAndAnOverflowAreSaidInWords() {
        assertThat(ReplicaMoves.duration(Count.exact(50), 10)).contains("5s");
        ReplicaLogs.Broker broker =
                new ReplicaLogs.Broker(Map.of(ORDERS, new ReplicaInfo(Long.MAX_VALUE, 0, false)), Map.of());
        Count bytes = ReplicaMoves.bytesToCopy(
                List.of(1),
                List.of(2, 3),
                new ReplicaLogs.Read(Map.of(1, broker), ""),
                ORDERS,
                new Node(1, "one", 9092));
        assertThat(bytes).isInstanceOf(Count.Unavailable.class);
        assertThat(ReplicaMoves.bytesToCopy(List.of(1), List.of(2), new ReplicaLogs.Read(Map.of(), ""), ORDERS, null))
                .isInstanceOf(Count.Unavailable.class);
    }

    @Test
    void aReplicaThatIsNotANumberOrIsRepeatedIsRefused() {
        ResourceId id = new ResourceId(KafkaKinds.PARTITION, ResourcePath.of("orders", "0"));
        Operation.Reassign text =
                new Operation.Reassign(Map.of(PartitionId.of("0"), List.of(NodeId.of("east"))), Attributes.empty());
        assertThatThrownBy(() -> ReplicaMoves.assignment(id, text)).isInstanceOf(BrokerException.class);
        Operation.Reassign twice = new Operation.Reassign(
                Map.of(PartitionId.of("0"), List.of(NodeId.of("1"), NodeId.of("1"))), Attributes.empty());
        assertThatThrownBy(() -> ReplicaMoves.assignment(id, twice)).isInstanceOf(BrokerException.class);
    }

    @Test
    void aFactorBelowMinInSyncNamesBothNumbers() {
        assertThatThrownBy(() -> ReplicaMoves.checkFactor(1, 2)).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
            assertThat(e.getMessage()).contains("1").contains("2");
        });
    }

    @Test
    void aRateReportedAsTheDefaultIsStillTheOverride() {
        ConfigEntry rate = new ConfigEntry(
                ReplicaMoves.LEADER_RATE,
                "1",
                ConfigEntry.ConfigSource.DEFAULT_CONFIG,
                false,
                true,
                List.of(),
                ConfigEntry.ConfigType.LONG,
                null);
        Config config = new Config(List.of(rate));

        assertThat(ReplicaMoves.override(config, ReplicaMoves.LEADER_RATE)).contains("1");
    }

    @Test
    void theUnlimitedRateIsNotAnOverride() {
        ConfigEntry rate = new ConfigEntry(
                ReplicaMoves.LEADER_RATE,
                "9223372036854775807",
                ConfigEntry.ConfigSource.DEFAULT_CONFIG,
                false,
                false,
                List.of(),
                ConfigEntry.ConfigType.LONG,
                null);

        assertThat(ReplicaMoves.override(new Config(List.of(rate)), ReplicaMoves.LEADER_RATE))
                .isEmpty();
    }

    private static Await await() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        return new Await(Deadline.after(Duration.ofSeconds(5), clock), clock, NodeId.of("cluster"));
    }

    private static KafkaCluster cluster() {
        ClusterSpec spec = new ClusterSpec(
                List.of(), List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, "127.0.0.1:1")), Map.of());
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
            public void evict(String key) {
                // This test does not keep a shared client, so there is nothing to drop.
            }
        };
        return new KafkaCluster(spec, CredentialSource.none(), context);
    }
}
