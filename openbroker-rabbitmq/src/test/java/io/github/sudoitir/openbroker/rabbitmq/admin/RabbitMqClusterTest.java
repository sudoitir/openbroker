package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RabbitMqClusterTest {

    private static RabbitMqCluster cluster(List<Endpoint> clusterEndpoints, List<NodeSpec> nodes) {
        return new RabbitMqCluster(
                new ClusterSpec(nodes, clusterEndpoints, Map.of()),
                CredentialSource.none(),
                new FakeManagement.TestContext(ConnectOptions.defaults()));
    }

    private static BrokerException refused(Endpoint endpoint) {
        try {
            cluster(List.of(endpoint), List.of());
        } catch (BrokerException e) {
            return e;
        }
        throw new AssertionError("expected the endpoint to be refused");
    }

    @Test
    void aManagementAddressIsABaseUrlWhoseSchemeMatchesItsProtocol() {
        for (String address :
                new String[] {"rabbit.example.com:15672", "https://h:1", "ftp://h:1", "http://", "http://u:p@h:1"}) {
            BrokerException e = refused(Endpoint.of(Plane.MANAGEMENT, "http", address));
            assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
            assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        }
        assertThat(refused(Endpoint.of(Plane.MANAGEMENT, "https", "http://h:1")).getMessage())
                .contains("https://");
        assertThat(refused(Endpoint.of(Plane.MANAGEMENT, "http", "http://u:pw-marker@h:1"))
                        .getMessage())
                .doesNotContain("pw-marker");
        assertThat(refused(Endpoint.of(Plane.MANAGEMENT, "http", "pw-marker@h:1"))
                        .getMessage())
                .doesNotContain("pw-marker");
    }

    @Test
    void validManagementAddressesAreKeptInOrderClusterFirstThenNodes() {
        NodeSpec node =
                new NodeSpec(NodeId.of("n1"), List.of(Endpoint.of(Plane.MANAGEMENT, "https", "https://n1:15671/pre/")));
        RabbitMqCluster cluster =
                cluster(List.of(Endpoint.of(Plane.MANAGEMENT, "http", "HTTP://front:80")), List.of(node));
        assertThat(cluster.managementTargets())
                .extracting(t -> t.base().toString())
                .containsExactly("HTTP://front:80", "https://n1:15671/pre");
        assertThat(cluster.managementTargets().get(1).node()).isEqualTo(NodeId.of("n1"));
        assertThat(cluster.managementTargets().getFirst().node()).isEqualTo(RabbitMqCluster.CLUSTER);
    }

    @Test
    void aDataAddressIsHostAndPortWithDefaults() {
        assertThat(RabbitMqCluster.dataAddress(Endpoint.of(Plane.DATA, "amqp", "h:5673")))
                .isEqualTo(new RabbitMqCluster.HostPort("h", 5673));
        assertThat(RabbitMqCluster.dataAddress(Endpoint.of(Plane.DATA, "amqp", "h")))
                .isEqualTo(new RabbitMqCluster.HostPort("h", 5672));
        assertThat(RabbitMqCluster.dataAddress(Endpoint.of(Plane.DATA, "amqps", "h")))
                .isEqualTo(new RabbitMqCluster.HostPort("h", 5671));
        assertThat(RabbitMqCluster.dataAddress(Endpoint.of(Plane.DATA, "amqp", "[::1]:5673")))
                .isEqualTo(new RabbitMqCluster.HostPort("[::1]", 5673));
        assertThat(RabbitMqCluster.dataAddress(Endpoint.of(Plane.DATA, "amqps", "[::1]")))
                .isEqualTo(new RabbitMqCluster.HostPort("[::1]", 5671));
    }

    @Test
    void aMalformedDataAddressIsRefusedWhenTheClusterIsSetUp() {
        for (String address :
                new String[] {"amqp://h:5672", "h:5672/x", "h:abc", "h:0", "h:70000", ":5672", "::1", "[::1]x", "[::1]:"
                }) {
            BrokerException e = refused(Endpoint.of(Plane.DATA, "amqp", address));
            assertThat(e.status()).as(address).isEqualTo(Status.INVALID_ARGUMENT);
            assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        }
        NodeSpec node = new NodeSpec(NodeId.of("n"), List.of(Endpoint.of(Plane.DATA, "amqp", "h:x")));
        assertThatThrownBy(() -> cluster(List.of(), List.of(node))).isInstanceOf(BrokerException.class);
    }

    @Test
    void trailingSlashesOfAManagementPathAreDroppedHoweverMany() {
        String slashes = "/".repeat(200_000);
        RabbitMqCluster cluster =
                cluster(List.of(Endpoint.of(Plane.MANAGEMENT, "http", "http://a:1/rabbit" + slashes)), List.of());
        assertThat(cluster.managementTargets().getFirst().base().toString()).isEqualTo("http://a:1/rabbit");
    }

    @Test
    void reconfigureRevalidatesDropsCachesAndNotifies() {
        RabbitMqCluster cluster = cluster(List.of(Endpoint.of(Plane.MANAGEMENT, "http", "http://a:1")), List.of());
        AtomicInteger changed = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        cluster.onReconfigure(changed::incrementAndGet);
        cluster.onClose(closed::incrementAndGet);
        ClusterSpec bad = new ClusterSpec(List.of(), List.of(Endpoint.of(Plane.MANAGEMENT, "http", "a:1")), Map.of());
        assertThatThrownBy(() -> cluster.reconfigure(bad)).isInstanceOf(BrokerException.class);
        assertThat(changed).hasValue(0);
        assertThat(cluster.managementTargets().getFirst().base().toString()).isEqualTo("http://a:1");
        ClusterSpec good =
                new ClusterSpec(List.of(), List.of(Endpoint.of(Plane.MANAGEMENT, "http", "http://b:2")), Map.of());
        cluster.reconfigure(good);
        assertThat(changed).hasValue(1);
        assertThat(cluster.spec()).isSameAs(good);
        assertThat(cluster.managementTargets().getFirst().base().toString()).isEqualTo("http://b:2");
        cluster.close();
        assertThat(closed).hasValue(1);
    }

    @Test
    void callRunsTheWorkUnlessTheDeadlinePassed() {
        RabbitMqCluster cluster = cluster(List.of(), List.of());
        Deadline open = Deadline.after(Duration.ofSeconds(5), Clock.systemUTC());
        assertThat(cluster.call("op", Optional.empty(), open, () -> "done")).isEqualTo("done");
        Deadline passed = Deadline.after(Duration.ofSeconds(-1), Clock.systemUTC());
        assertThatThrownBy(() -> cluster.call("op", Optional.empty(), passed, () -> "never"))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
                    assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
                });
    }
}
