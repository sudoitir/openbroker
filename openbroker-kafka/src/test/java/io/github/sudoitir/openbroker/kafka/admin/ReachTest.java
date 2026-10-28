package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Membership;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.NodeFunction;
import io.github.sudoitir.openbroker.model.Serving;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** The checks of Reach that need no broker: names that do not resolve, closed ports, and nodes that are no broker. */
class ReachTest {

    private static DriverContext context(ConnectOptions options) {
        return new DriverContext() {
            @Override
            public ConnectOptions options() {
                return options;
            }

            @Override
            public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
                return factory.get();
            }

            @Override
            public void evict(String key) {}
        };
    }

    private static NodeDescription node(String id, Set<NodeFunction> functions, String address) {
        return new NodeDescription(
                NodeId.of(id),
                id,
                Membership.standalone(Serving.ACTIVE),
                functions,
                List.of(new Endpoint(Plane.DATA, KafkaOptions.PROTOCOL, address)),
                Optional.empty(),
                List.of(),
                Attributes.empty(),
                Optional.empty());
    }

    private static Partial<List<NodeDescription>> reach(NodeDescription... nodes) {
        ClusterSpec spec = new ClusterSpec(
                List.of(), List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, "localhost:1")), Map.of());
        try (KafkaCluster cluster =
                new KafkaCluster(spec, CredentialSource.none(), context(ConnectOptions.defaults()))) {
            List<NodeDescription> all = List.of(nodes);
            Partial<List<NodeDescription>> described = Partial.complete(
                    all, all.stream().map(NodeDescription::node).toList());
            return Reach.of(
                    cluster,
                    described,
                    Deadline.after(Duration.ofSeconds(10), cluster.options().clock()));
        }
    }

    @Test
    void aNameThatDoesNotResolveIsAFailedBrokerThatSaysSo() {
        NodeDescription unresolvable = node("2", EnumSet.of(NodeFunction.BROKER), "no-such-broker.invalid:9092");

        Partial<List<NodeDescription>> result = reach(unresolvable);

        assertThat(result.outcomes()).containsEntry(NodeId.of("2"), NodeOutcome.FAILED);
        assertThat(result.value()).containsExactly(unresolvable);
        BrokerException failure = result.failures().get(NodeId.of("2"));
        assertThat(failure.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(failure.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        assertThat(failure.getMessage())
                .isEqualTo("Broker 2 advertises no-such-broker.invalid:9092, which does not resolve.");
        assertThat(failure.info().metadata())
                .containsEntry(Reach.ENDPOINT, "no-such-broker.invalid:9092")
                .containsEntry(Reach.FAILURE, "dns");
    }

    @Test
    void aPortNothingListensOnIsAFailedBrokerThatRefuses() throws IOException {
        int closed;
        try (ServerSocket probe = new ServerSocket(0)) {
            closed = probe.getLocalPort();
        }
        NodeDescription refusing = node("1", EnumSet.of(NodeFunction.BROKER), "127.0.0.1:" + closed);

        Partial<List<NodeDescription>> result = reach(refusing);

        assertThat(result.outcomes()).containsEntry(NodeId.of("1"), NodeOutcome.FAILED);
        assertThat(result.failures().get(NodeId.of("1")).info().metadata()).containsEntry(Reach.FAILURE, "refused");
        assertThat(result.failures().get(NodeId.of("1")).getMessage()).contains("refuses connections");
    }

    @Test
    void aControllerThatIsNoBrokerIsSkippedAndKeepsItsDescription() {
        NodeDescription controller = node("9", EnumSet.of(NodeFunction.CONTROLLER), "controller:9093");

        Partial<List<NodeDescription>> result = reach(controller);

        assertThat(result.outcomes()).containsEntry(NodeId.of("9"), NodeOutcome.SKIPPED);
        assertThat(result.failures()).isEmpty();
        assertThat(result.value()).containsExactly(controller);
    }

    @Test
    void oneUnreachedBrokerLeavesTheOthersDescribed() {
        NodeDescription controller = node("9", EnumSet.of(NodeFunction.CONTROLLER), "controller:9093");
        NodeDescription lost = node("2", EnumSet.of(NodeFunction.BROKER), "no-such-broker.invalid:9092");

        Partial<List<NodeDescription>> result = reach(controller, lost);

        assertThat(result.value()).containsExactly(controller, lost);
        assertThat(result.outcomes())
                .containsEntry(NodeId.of("9"), NodeOutcome.SKIPPED)
                .containsEntry(NodeId.of("2"), NodeOutcome.FAILED);
    }
}
