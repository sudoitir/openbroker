package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.ClusterMember;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Serving;
import io.github.sudoitir.openbroker.model.Shard;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RabbitMqNodesTest {

    private FakeManagement server;
    private RabbitMqNodes nodes;

    @BeforeEach
    void start() {
        server = new FakeManagement();
        server.json("GET", "/api/overview", "overview.json").json("GET", "/api/nodes", "nodes.json");
        nodes = new RabbitMqNodes(FakeManagement.cluster(server.endpoint()));
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private static NodeDescription byName(List<NodeDescription> all, String name) {
        return all.stream().filter(n -> n.nativeId().equals(name)).findFirst().orElseThrow();
    }

    private static Condition condition(NodeDescription node, String type) {
        return node.conditions().stream()
                .filter(c -> c.type().equals(type))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void describesEveryNodeByItsNativeName() {
        List<NodeDescription> all = nodes.describe(Scope.whole()).value();
        assertThat(all)
                .extracting(NodeDescription::nativeId)
                .containsExactly("rabbit@alpha", "rabbit@beta", "rabbit@gamma");
        NodeDescription alpha = byName(all, "rabbit@alpha");
        assertThat(alpha.node()).isEqualTo(NodeId.of("rabbit@alpha"));
        assertThat(alpha.version()).contains("3.13.7");
        assertThat(alpha.membership().serving()).isEqualTo(Serving.ACTIVE);
        assertThat(alpha.membership().group()).contains("rabbit@alpha");
        assertThat(alpha.attributes().raw(RabbitMqNodes.PRODUCT_NAME)).contains("RabbitMQ");
        assertThat(alpha.attributes().raw(RabbitMqNodes.PRODUCT_VERSION)).contains("3.13.7");
    }

    @Test
    void eachNodeShowsItsOwnVersionDuringARollingUpgrade() {
        assertThat(byName(nodes.describe(Scope.whole()).value(), "rabbit@beta").version())
                .contains("3.13.6");
    }

    @Test
    void mapsListenersToEndpointsOfTheDriversProtocols() {
        List<NodeDescription> all = nodes.describe(Scope.whole()).value();
        assertThat(byName(all, "rabbit@alpha").endpoints())
                .containsExactly(
                        Endpoint.of(Plane.DATA, "amqp", "alpha:5672"),
                        Endpoint.of(Plane.MANAGEMENT, "http", "http://alpha:15672"));
        assertThat(byName(all, "rabbit@beta").endpoints())
                .containsExactly(
                        Endpoint.of(Plane.DATA, "amqps", "beta:5671"),
                        Endpoint.of(Plane.MANAGEMENT, "https", "https://beta:15671"),
                        Endpoint.of(Plane.DATA, "stream", "beta:5552"));
        assertThat(byName(all, "rabbit@gamma").endpoints()).isEmpty();
    }

    @Test
    void anAlarmMakesTheNodeFull() {
        List<NodeDescription> all = nodes.describe(Scope.whole()).value();
        assertThat(condition(byName(all, "rabbit@beta"), ConditionTypes.FULL).status())
                .isEqualTo(ConditionStatus.TRUE);
        assertThat(condition(byName(all, "rabbit@beta"), ConditionTypes.FULL).reason())
                .isEqualTo("mem_alarm");
        assertThat(condition(byName(all, "rabbit@alpha"), ConditionTypes.FULL).status())
                .isEqualTo(ConditionStatus.FALSE);
    }

    @Test
    void aNodeThatIsNotRunningIsNotServing() {
        NodeDescription gamma = byName(nodes.describe(Scope.whole()).value(), "rabbit@gamma");
        assertThat(condition(gamma, ConditionTypes.SERVING).status()).isEqualTo(ConditionStatus.FALSE);
        assertThat(gamma.membership().serving()).isEqualTo(Serving.UNKNOWN);
        assertThat(gamma.version()).isEmpty();
    }

    @Test
    void aPartitionNamesTheNodesItIsCutOffFrom() {
        NodeDescription beta = byName(nodes.describe(Scope.whole()).value(), "rabbit@beta");
        Condition partition = condition(beta, ConditionTypes.OUT_OF_SYNC);
        assertThat(partition.status()).isEqualTo(ConditionStatus.TRUE);
        assertThat(partition.message()).contains("rabbit@gamma");
        assertThat(condition(byName(nodes.describe(Scope.whole()).value(), "rabbit@alpha"), ConditionTypes.OUT_OF_SYNC)
                        .status())
                .isEqualTo(ConditionStatus.FALSE);
    }

    @Test
    void aScopeSelectsNodes() {
        assertThat(nodes.describe(Scope.node(NodeId.of("rabbit@beta"))).value())
                .extracting(NodeDescription::nativeId)
                .containsExactly("rabbit@beta");
        assertThatThrownBy(() -> nodes.describe(Scope.node(NodeId.of("rabbit@nowhere"))))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.NOT_FOUND));
        assertThatThrownBy(() -> nodes.describe(Scope.shards(Set.of(Shard.partition(PartitionId.of("0"))))))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }

    @Test
    void membersArePerNodeWithTheirDataEndpoint() {
        List<ClusterMember> members = nodes.members(Scope.whole()).value();
        assertThat(members)
                .extracting(ClusterMember::nativeId)
                .containsExactly("rabbit@alpha", "rabbit@beta", "rabbit@gamma");
        assertThat(members.getFirst().replicas()).hasSize(1);
        assertThat(members.getFirst().replicas().getFirst().endpoint().protocol())
                .isEqualTo("amqp");
        assertThat(members.getLast().replicas()).isEmpty();
    }

    @Test
    void aNodeListThatIsNotAListIsABadResponse() {
        try (FakeManagement other = new FakeManagement()) {
            other.json("GET", "/api/overview", "overview.json").respond("GET", "/api/nodes", 200, Map.of(), "{}");
            RabbitMqNodes broken = new RabbitMqNodes(FakeManagement.cluster(other.endpoint()));
            assertThatThrownBy(() -> broken.describe(Scope.whole())).isInstanceOf(BrokerException.class);
        }
    }
}
