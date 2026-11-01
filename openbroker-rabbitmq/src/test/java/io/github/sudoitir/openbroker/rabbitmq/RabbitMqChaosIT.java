package io.github.sudoitir.openbroker.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.Timeouts;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendBuffer;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.rabbitmq.messaging.RabbitMqTransport;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.tck.ChaosTck;
import io.github.sudoitir.openbroker.tck.TckChaos;
import io.github.sudoitir.openbroker.tck.TckEnvironment;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The chaos TCK against a three-node cluster of the image the build names ({@code rabbitmq.image}), every node behind
 * Toxiproxy on its AMQP and its management port. The cluster as a whole is the TCK's node, as links and calls are
 * attributed to {@link RabbitMqCluster#CLUSTER}: cutting it holds the traffic to every node, and killing it stops
 * them all; a node named by {@code serving} or {@code successor} is acted on alone. The serving node of a
 * subscription is its quorum queue's leader and the successor an online follower.
 *
 * <p>Beside the TCK it runs the cases that only RabbitMQ has: a node lost while the quorum survives, a receiver cut
 * off with messages unacknowledged, and a management API that is slow, reset or silent.
 */
class RabbitMqChaosIT extends ChaosTck {

    private static final Duration WAIT = Duration.ofSeconds(120);
    private static final Duration CALL = Duration.ofSeconds(2);

    private static RabbitMqChaosCluster cluster;

    @BeforeAll
    static void startCluster() {
        cluster = new RabbitMqChaosCluster(
                RabbitMqTestCluster.image(), System.getProperty("toxiproxy.image", "ghcr.io/shopify/toxiproxy:2.12.0"));
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    @Override
    protected TckEnvironment environment() {
        return new Environment();
    }

    @Override
    protected TckChaos chaos() {
        cluster.heal(RabbitMqChaosCluster.all());
        cluster.start(RabbitMqChaosCluster.all());
        return new TckChaos() {
            @Override
            public ConnectRequest request() {
                return RabbitMqChaosIT.request();
            }

            @Override
            public ConnectRequest haRequest() {
                return request();
            }

            @Override
            public NodeId node() {
                return RabbitMqCluster.CLUSTER;
            }

            @Override
            public NodeId serving(ResourceId subscription) {
                return cluster.leader(queue(subscription));
            }

            @Override
            public NodeId successor(ResourceId subscription) {
                return cluster.follower(queue(subscription));
            }

            @Override
            public boolean linksNameTheirServingNode() {
                return false;
            }

            @Override
            public void cut(NodeId node) {
                cluster.cut(members(node));
            }

            @Override
            public void heal(NodeId node) {
                cluster.heal(members(node));
            }

            @Override
            public void kill(NodeId node) {
                cluster.kill(members(node));
            }

            @Override
            public void start(NodeId node) {
                cluster.start(members(node));
            }
        };
    }

    /** The nodes a node stands for: the whole cluster, or one node. */
    private static List<NodeId> members(NodeId node) {
        return node.equals(RabbitMqCluster.CLUSTER) ? RabbitMqChaosCluster.all() : List.of(node);
    }

    private static String queue(ResourceId subscription) {
        return subscription.path().segments().get(1);
    }

    private static ConnectRequest request() {
        return new ConnectRequest(
                RabbitMqDriver.SYSTEM,
                cluster.spec(),
                CredentialSource.of(Credentials.usernamePassword(
                        RabbitMqTestCluster.ADMIN_USER, RabbitMqTestCluster.ADMIN_PASSWORD)),
                ConnectOptions.defaults().withTimeouts(Timeouts.fixed(Duration.ofSeconds(1), CALL)));
    }

    /** What the TCK asks of the driver: a subscription is a durable quorum queue on the default virtual host. */
    private static final class Environment implements TckEnvironment {
        @Override
        public BrokerDriver driver() {
            return new RabbitMqDriver();
        }

        @Override
        public ConnectRequest request() {
            return RabbitMqChaosIT.request();
        }

        @Override
        public ConnectRequest requestWithWrongPassword(String wrongPassword) {
            ConnectRequest request = RabbitMqChaosIT.request();
            return new ConnectRequest(
                    request.system(),
                    request.cluster(),
                    CredentialSource.of(Credentials.usernamePassword(RabbitMqTestCluster.ADMIN_USER, wrongPassword)),
                    request.options());
        }

        @Override
        public KindId subscriptionKind() {
            return RabbitMqKinds.QUORUM_QUEUE;
        }

        @Override
        public ResourceId subscriptionId(String name) {
            return ResourceId.of(RabbitMqKinds.QUORUM_QUEUE.value(), "/", name);
        }

        @Override
        public Attributes subscriptionAttributes(String name) {
            return Attributes.builder().put(StandardAttributes.DURABLE, true).build();
        }

        @Override
        public MessageFilter nativeEquals(String name, String value) {
            return new MessageFilter(nativeDialect(), name + "=" + value);
        }

        @Override
        public String nativeDialect() {
            // A queue filters nowhere, so no case that needs a native filter runs.
            return "rabbitmq.none";
        }
    }

    // Cases beside the TCK, on the facets directly.

    private record Context(ConnectOptions options) implements DriverContext {
        @Override
        public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
            return factory.get();
        }

        @Override
        public void evict(String key) {
            // Nothing is kept.
        }
    }

    private static RabbitMqCluster connect() {
        ConnectRequest request = request();
        return new RabbitMqCluster(request.cluster(), request.credentials(), new Context(request.options()));
    }

    private static OutboundMessage text(String body) {
        return new OutboundMessage(MessageHeaders.empty(), Map.of(), MessageBody.of(body.getBytes()), Map.of());
    }

    private static String body(Message message) {
        return new String(message.body().bytes().orElseThrow().toByteArray());
    }

    private static ResourceId id(String name) {
        return ResourceId.of(RabbitMqKinds.QUORUM_QUEUE.value(), "/", name);
    }

    /** Receives until {@code count} distinct payloads are seen, settling each; returns every payload received. */
    private static List<String> drain(Receiver receiver, int count) {
        List<String> received = new ArrayList<>();
        Set<String> distinct = new HashSet<>();
        RabbitMq.eventually(
                WAIT,
                () -> {
                    for (Message message : receiver.receive(50, Duration.ofSeconds(1))) {
                        received.add(body(message));
                        distinct.add(body(message));
                        receiver.settle(message, Disposition.accept());
                    }
                    return distinct.size();
                },
                n -> n >= count);
        return received;
    }

    @Test
    void aNodeLostMidSendLosesNoConfirmedMessageAndEveryDuplicateIsReportedResent() throws Exception {
        String name = "chaos-node-" + UUID.randomUUID().toString().substring(0, 8);
        cluster.declareQuorumQueue(name);
        int count = 300;
        try (RabbitMqCluster rabbit = connect();
                RabbitMqTransport transport = new RabbitMqTransport(rabbit);
                Sender sender = transport.sender(
                        SendTarget.subscription(id(name)),
                        Scope.whole(),
                        SenderOptions.defaults().withBuffer(new SendBuffer(500, 16L * 1024 * 1024, WAIT)));
                Receiver receiver = transport.receiver(id(name), Scope.whole(), ReceiverOptions.consume())) {
            NodeId leader = cluster.leader(name);
            List<CompletableFuture<SendReceipt>> receipts = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (i == count / 3) {
                    cluster.kill(List.of(leader));
                }
                if (sender.offer(text(Integer.toString(i))) instanceof Offer.Accepted accepted) {
                    receipts.add(accepted.receipt().toCompletableFuture());
                } else {
                    throw new AssertionError("The buffer is roomy and still refused message " + i);
                }
            }
            cluster.start(List.of(leader));
            CompletableFuture.allOf(receipts.toArray(CompletableFuture[]::new)).get(WAIT.toSeconds(), TimeUnit.SECONDS);

            List<String> stored = drain(receiver, count);
            Set<String> expected = new HashSet<>();
            for (int i = 0; i < count; i++) {
                expected.add(Integer.toString(i));
            }
            assertThat(new HashSet<>(stored)).isEqualTo(expected);
            Map<String, Integer> copies = new HashMap<>();
            stored.forEach(n -> copies.merge(n, 1, Integer::sum));
            copies.forEach((n, times) -> {
                if (times > 1) {
                    assertThat(receipts.get(Integer.parseInt(n)).join().resent())
                            .as("message %s is stored %d times and says it was not sent again", n, times)
                            .isTrue();
                }
            });
        } finally {
            cluster.deleteQueue(name);
        }
    }

    @Test
    void aCutReceiverIsReopenedAndItsUnacknowledgedMessagesAreDeliveredAgain() {
        String name = "chaos-cut-" + UUID.randomUUID().toString().substring(0, 8);
        cluster.declareQuorumQueue(name);
        int count = 5;
        List<LinkEvent> events = new CopyOnWriteArrayList<>();
        try (RabbitMqCluster rabbit = connect();
                RabbitMqTransport transport = new RabbitMqTransport(rabbit);
                AutoCloseable ignored = transport.watch(events::add);
                Receiver receiver = transport.receiver(id(name), Scope.whole(), ReceiverOptions.consume())) {
            for (int i = 0; i < count; i++) {
                cluster.publish(name, Integer.toString(i));
            }
            Set<String> first = new HashSet<>();
            RabbitMq.eventually(
                    WAIT,
                    () -> {
                        receiver.receive(count, Duration.ofSeconds(1)).forEach(m -> first.add(body(m)));
                        return first.size();
                    },
                    n -> n == count);

            cluster.cut(RabbitMqChaosCluster.all());
            RabbitMq.eventually(WAIT, receiver::state, state -> state == LinkState.RECOVERING);
            cluster.heal(RabbitMqChaosCluster.all());
            RabbitMq.eventually(WAIT, receiver::state, state -> state == LinkState.OPEN);

            List<Message> again = new ArrayList<>();
            RabbitMq.eventually(
                    WAIT,
                    () -> {
                        again.addAll(receiver.receive(count, Duration.ofSeconds(1)));
                        return again.size();
                    },
                    n -> n >= count);
            assertThat(again.stream().map(RabbitMqChaosIT::body).toList()).containsAll(first);
            assertThat(again)
                    .allSatisfy(m -> assertThat(m.extension("rabbitmq").raw("redelivered"))
                            .contains(true));
            again.forEach(m -> receiver.settle(m, Disposition.accept()));
            assertThat(RabbitMq.eventually(WAIT, () -> cluster.messages(name), n -> n == 0))
                    .isZero();
            assertThat(events.stream().map(LinkEvent::state).toList())
                    .containsSubsequence(LinkState.RECOVERING, LinkState.OPEN);
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            cluster.deleteQueue(name);
        }
    }

    @Test
    void aManagementCallFailsOverFromARefusingEndpointToTheNextNode() throws IOException {
        // A refused connection is a failure before anything was sent, the only one the next endpoint is tried after.
        int closed;
        try (ServerSocket socket = new ServerSocket(0)) {
            closed = socket.getLocalPort();
        }
        List<NodeSpec> nodes = new ArrayList<>();
        nodes.add(new NodeSpec(
                NodeId.of("refusing"),
                List.of(Endpoint.of(Plane.MANAGEMENT, RabbitMqOptions.HTTP, "http://127.0.0.1:" + closed))));
        nodes.addAll(cluster.spec().nodes());
        ConnectRequest request = request();
        try (RabbitMqCluster rabbit = new RabbitMqCluster(
                new ClusterSpec(nodes, List.of(), Map.of()), request.credentials(), new Context(request.options()))) {
            var response = rabbit.management().get("/api/overview", Map.of(), deadline(rabbit, WAIT), "chaos.overview");

            assertThat(response.body().path("cluster_name").asString()).isNotEmpty();
        }
    }

    @Test
    void aManagementCallToSilentEndpointsKeepsItsDeadline() throws IOException {
        for (int i = 0; i < 3; i++) {
            cluster.managementProxy(i).toxics().timeout("silent", ToxicDirection.DOWNSTREAM, 0);
        }
        Duration within = Duration.ofSeconds(3);
        try (RabbitMqCluster rabbit = connect()) {
            long started = System.nanoTime();
            assertThatThrownBy(() -> rabbit.management()
                            .get("/api/overview", Map.of(), deadline(rabbit, within), "chaos.overview"))
                    .isInstanceOfSatisfying(
                            BrokerException.class,
                            e -> assertThat(e.status()).isIn(Status.DEADLINE_EXCEEDED, Status.UNAVAILABLE));
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(within.plusSeconds(3));
        }
    }

    @Test
    void aSlowManagementEndpointStillAnswersWithinTheDeadline() throws IOException {
        Duration delay = Duration.ofMillis(800);
        cluster.managementProxy(0).toxics().latency("slow", ToxicDirection.DOWNSTREAM, delay.toMillis());
        try (RabbitMqCluster rabbit = connect()) {
            long started = System.nanoTime();
            var response = rabbit.management().get("/api/overview", Map.of(), deadline(rabbit, WAIT), "chaos.overview");

            assertThat(response.body().path("cluster_name").asString()).isNotEmpty();
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isGreaterThanOrEqualTo(delay);
        }
    }

    private static Deadline deadline(RabbitMqCluster rabbit, Duration within) {
        return Deadline.after(within, rabbit.options().clock());
    }
}
