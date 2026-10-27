package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.Timeouts;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.kafka.admin.KafkaNodes;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.NodeFunction;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The driver against a real three-broker cluster: its nodes, its quorum and its access. */
class KafkaConnectionIT {

    private static KafkaTestCluster kafka;
    private static BrokerDrivers drivers;

    @BeforeAll
    static void start() {
        kafka = KafkaTestCluster.start();
        drivers = BrokerDrivers.load();
    }

    @AfterAll
    static void stop() {
        drivers.close();
        kafka.close();
    }

    private static ConnectRequest request(ClusterSpec spec, ConnectOptions options) {
        return new ConnectRequest(KafkaDriver.SYSTEM, spec, CredentialSource.none(), options);
    }

    @Test
    void verifyReportsEveryBrokerAsANodeAndTheQuorumInTheirHealth() {
        ConnectionReport report = drivers.verify(request(kafka.spec(), ConnectOptions.defaults()));

        assertThat(report.nodes().isComplete()).isTrue();
        List<NodeDescription> nodes = report.nodes().value();
        assertThat(nodes).extracting(NodeDescription::nativeId).containsExactlyInAnyOrder("1", "2", "3");
        assertThat(nodes).allSatisfy(node -> {
            assertThat(node.functions()).containsExactlyInAnyOrder(NodeFunction.BROKER, NodeFunction.CONTROLLER);
            assertThat(node.attributes().raw(KafkaNodes.QUORUM_VOTER)).contains(true);
            assertThat(node.attributes().raw(KafkaNodes.CLUSTER_ID)).isPresent();
            assertThat(node.conditions())
                    .anySatisfy(c -> {
                        assertThat(c.type()).isEqualTo(ConditionTypes.SERVING);
                        assertThat(c.status()).isEqualTo(ConditionStatus.TRUE);
                    })
                    .anySatisfy(c -> {
                        assertThat(c.type()).isEqualTo(ConditionTypes.UNDER_REPLICATED);
                        assertThat(c.status()).isEqualTo(ConditionStatus.FALSE);
                    });
        });
        assertThat(nodes)
                .filteredOn(n -> n.attributes().raw(KafkaNodes.QUORUM_LEADER).equals(Optional.of(true)))
                .hasSize(1);
        assertThat(report.access().of(Capability.MANAGEMENT_READ).status()).isEqualTo(Assessment.Status.AVAILABLE);
        assertThat(report.access().of(Capability.MANAGEMENT_WRITE).status()).isEqualTo(Assessment.Status.UNKNOWN);
    }

    @Test
    void aNodeCarriesItsDataEndpoint() {
        try (ClusterConnection connection = drivers.connect(request(kafka.spec(), ConnectOptions.defaults()))) {
            NodeDescription node = connection
                    .nodes()
                    .describe(Scope.node(NodeId.of("2")))
                    .orThrow()
                    .getFirst();

            assertThat(node.endpoints()).singleElement().satisfies(e -> {
                assertThat(e.plane()).isEqualTo(Plane.DATA);
                assertThat(e.protocol()).isEqualTo(KafkaOptions.PROTOCOL);
            });
        }
    }

    @Test
    void aScopeNamesNodesAndAnUnknownNodeFailsAlone() {
        try (ClusterConnection connection = drivers.connect(request(kafka.spec(), ConnectOptions.defaults()))) {
            var result = connection
                    .nodes()
                    .describe(Scope.shards(Set.of(Shard.node(NodeId.of("1")), Shard.node(NodeId.of("99")))));

            assertThat(result.value()).extracting(NodeDescription::nativeId).containsExactly("1");
            assertThat(result.outcomes())
                    .containsEntry(NodeId.of("1"), NodeOutcome.APPLIED)
                    .containsEntry(NodeId.of("99"), NodeOutcome.FAILED);
            assertThat(result.failures().get(NodeId.of("99")).status()).isEqualTo(Status.NOT_FOUND);
        }
    }

    @Test
    void anyOneBrokerIsEnoughToReachTheCluster() {
        ClusterSpec one = new ClusterSpec(
                List.of(),
                List.of(Endpoint.of(
                        Plane.DATA,
                        KafkaOptions.PROTOCOL,
                        kafka.brokers().get(2).getBootstrapServers())),
                Map.of(KafkaOptions.CLIENT_ID, "openbroker-it"));

        assertThat(drivers.verify(request(one, ConnectOptions.defaults()))
                        .nodes()
                        .value())
                .hasSize(3);
    }

    @Test
    void anUnreachableClusterFailsWithinTheCallTimeout() {
        ClusterSpec nowhere = new ClusterSpec(
                List.of(), List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, "127.0.0.1:1")), Map.of());
        ConnectOptions quick =
                ConnectOptions.defaults().withTimeouts(Timeouts.fixed(Duration.ofSeconds(1), Duration.ofSeconds(3)));

        assertThatThrownBy(() -> drivers.verify(request(nowhere, quick)))
                .isInstanceOfSatisfying(
                        BrokerException.class,
                        e -> assertThat(e.status()).isIn(Status.UNAVAILABLE, Status.DEADLINE_EXCEEDED));
    }
}
