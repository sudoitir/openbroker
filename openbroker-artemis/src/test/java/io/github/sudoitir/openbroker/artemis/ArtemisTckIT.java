package io.github.sudoitir.openbroker.artemis;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.RouteEndpoint;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.tck.OpenBrokerTck;
import io.github.sudoitir.openbroker.tck.TckEnvironment;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** The TCK against one Artemis broker of the image the build names ({@code artemis.image}). */
@Testcontainers
class ArtemisTckIT extends OpenBrokerTck {

    private static final String USER = "artemis";
    private static final String PASSWORD = "artemis";

    @Container
    static final GenericContainer<?> BROKER = new GenericContainer<>(
                    System.getProperty("artemis.image", "apache/artemis:2.57.0"))
            .withEnv("ARTEMIS_USER", USER)
            .withEnv("ARTEMIS_PASSWORD", PASSWORD)
            .withEnv("ANONYMOUS_LOGIN", "false")
            .withExposedPorts(61616, 8161)
            .waitingFor(
                    Wait.forLogMessage(".*Artemis Console available.*", 1).withStartupTimeout(Duration.ofMinutes(2)));

    @Override
    protected TckEnvironment environment() {
        return new TckEnvironment() {
            @Override
            public BrokerDriver driver() {
                return new ArtemisDriver();
            }

            @Override
            public ConnectRequest request() {
                return connectTo(PASSWORD);
            }

            @Override
            public ConnectRequest requestWithWrongPassword(String wrongPassword) {
                return connectTo(wrongPassword);
            }

            @Override
            public KindId subscriptionKind() {
                return ArtemisKinds.QUEUE;
            }

            @Override
            public ResourceId subscriptionId(String name) {
                return new ResourceId(ArtemisKinds.QUEUE, ResourcePath.of(name));
            }

            @Override
            public Attributes subscriptionAttributes(String name) {
                return Attributes.builder()
                        .put(StandardAttributes.DURABLE, true)
                        .build();
            }

            @Override
            public MessageFilter nativeEquals(String name, String value) {
                return ArtemisFilters.selector(name + " = '" + value.replace("'", "''") + "'");
            }

            @Override
            public String nativeDialect() {
                return ArtemisFilters.DIALECT;
            }
        };
    }

    /** A node whose Core endpoint cannot be reached from here still takes a send, over management. */
    @Test
    void aSendFallsBackToManagementWhenCoreCannotBeReached() {
        String host = BROKER.getHost();
        NodeSpec node = new NodeSpec(
                NodeId.of("primary"),
                List.of(
                        Endpoint.of(
                                Plane.MANAGEMENT,
                                "jolokia",
                                "http://" + host + ":" + BROKER.getMappedPort(8161) + "/console/jolokia"),
                        Endpoint.of(Plane.DATA, "core", "tcp://127.0.0.1:1")));
        try (BrokerDrivers drivers = BrokerDrivers.of(List.of(new ArtemisDriver()));
                ClusterConnection connection = drivers.connect(new ConnectRequest(
                        "artemis",
                        ClusterSpec.ofNodes(List.of(node)),
                        CredentialSource.of(Credentials.usernamePassword(USER, PASSWORD)),
                        ConnectOptions.defaults()))) {
            ResourceId queue = new ResourceId(
                    ArtemisKinds.QUEUE,
                    ResourcePath.of("fallback." + UUID.randomUUID().toString().substring(0, 8)));
            connection
                    .operations()
                    .execute(
                            queue,
                            new Operation.Create(Attributes.builder()
                                    .put(StandardAttributes.DURABLE, true)
                                    .build()),
                            Scope.whole())
                    .orThrow();
            try {
                OutboundMessage message = new OutboundMessage(
                        MessageHeaders.builder().build(),
                        Map.of(),
                        MessageBody.of(Blob.of("hi".getBytes(StandardCharsets.UTF_8))),
                        Map.of());

                connection.messages().send(SendTarget.subscription(queue), message);

                // The broker counts a durable message once its store has written it, a moment after the send.
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                Count count;
                do {
                    count = connection
                            .messages()
                            .count(queue, Scope.whole(), Optional.empty())
                            .orThrow()
                            .count();
                } while (!count.equals(Count.exact(1)) && System.nanoTime() < until);
                assertThat(count).isEqualTo(Count.exact(1));
            } finally {
                connection
                        .operations()
                        .execute(queue, new Operation.Delete(false, true), Scope.whole())
                        .orThrow();
            }
        }
    }

    /** A divert is created once, refused when it would differ, and deleted, each with an honest outcome. */
    @Test
    void aDivertIsDeployedOnceRefusedWhenDifferentAndDeleted() {
        String run = UUID.randomUUID().toString().substring(0, 8);
        ResourceId from = new ResourceId(ArtemisKinds.ADDRESS, ResourcePath.of("divert.from." + run));
        ResourceId to = new ResourceId(ArtemisKinds.ADDRESS, ResourcePath.of("divert.to." + run));
        ResourceId other = new ResourceId(ArtemisKinds.ADDRESS, ResourcePath.of("divert.other." + run));
        ResourceId divert = new ResourceId(ArtemisKinds.DIVERT, ResourcePath.of("divert." + run));
        try (BrokerDrivers drivers = BrokerDrivers.of(List.of(new ArtemisDriver()));
                ClusterConnection connection = drivers.connect(connectTo(PASSWORD))) {
            Operations operations = connection.operations();
            Attributes multicast = Attributes.builder()
                    .putRaw(ArtemisKinds.ROUTING_TYPES, "MULTICAST")
                    .build();
            for (ResourceId address : List.of(from, to, other)) {
                operations
                        .execute(address, new Operation.Create(multicast), Scope.whole())
                        .orThrow();
            }
            try {
                Operation create = new Operation.Create(divertTo(from, to));

                assertThat(operations
                                .execute(divert, create, Scope.whole())
                                .orThrow()
                                .alreadyInState())
                        .isFalse();
                assertThat(operations
                                .execute(divert, create, Scope.whole())
                                .orThrow()
                                .alreadyInState())
                        .as("the same divert again is already in place")
                        .isTrue();
                Partial<OperationResult> different =
                        operations.execute(divert, new Operation.Create(divertTo(from, other)), Scope.whole());
                assertThat(different.failures().values()).singleElement().satisfies(e -> {
                    assertThat(e.status()).isEqualTo(Status.ALREADY_EXISTS);
                    assertThat(e.info().metadata()).containsEntry("differing", "forwarding-address");
                });
                assertThat(connection
                                .resources()
                                .read(ResourceQuery.of(ArtemisKinds.DIVERT))
                                .orThrow()
                                .items())
                        .anySatisfy(r -> assertThat(r.id()).isEqualTo(divert));

                assertThat(operations
                                .execute(divert, new Operation.Delete(false, false), Scope.whole())
                                .orThrow()
                                .alreadyInState())
                        .isFalse();
                assertThat(operations
                                .execute(divert, new Operation.Delete(false, false), Scope.whole())
                                .orThrow()
                                .alreadyInState())
                        .isTrue();
            } finally {
                operations.execute(divert, new Operation.Delete(false, false), Scope.whole());
                for (ResourceId address : List.of(from, to, other)) {
                    operations.execute(address, new Operation.Delete(false, true), Scope.whole());
                }
            }
        }
    }

    /** A queue asked to create its address gets one; the same request again is already in place. */
    @Test
    void aQueueCanCreateItsAddressAndIsAlreadyThereTheSecondTime() {
        String run = UUID.randomUUID().toString().substring(0, 8);
        ResourceId address = new ResourceId(ArtemisKinds.ADDRESS, ResourcePath.of("auto.address." + run));
        ResourceId queue = new ResourceId(ArtemisKinds.QUEUE, ResourcePath.of("auto.queue." + run));
        Operation create = new Operation.Create(Attributes.builder()
                .put(StandardAttributes.DESTINATION, address)
                .putRaw(ArtemisKinds.ROUTING_TYPE, "ANYCAST")
                .putRaw(ArtemisKinds.AUTO_CREATE_ADDRESS, true)
                .build());
        try (BrokerDrivers drivers = BrokerDrivers.of(List.of(new ArtemisDriver()));
                ClusterConnection connection = drivers.connect(connectTo(PASSWORD))) {
            Operations operations = connection.operations();
            try {
                assertThat(operations
                                .execute(queue, create, Scope.whole())
                                .orThrow()
                                .alreadyInState())
                        .isFalse();
                assertThat(operations
                                .execute(queue, create, Scope.whole())
                                .orThrow()
                                .alreadyInState())
                        .isTrue();
            } finally {
                operations.execute(queue, new Operation.Delete(false, true), Scope.whole());
                operations.execute(address, new Operation.Delete(false, true), Scope.whole());
            }
        }
    }

    private static Attributes divertTo(ResourceId from, ResourceId to) {
        return Attributes.builder()
                .put(StandardAttributes.ROUTE_SOURCE, new RouteEndpoint.Local(from))
                .put(StandardAttributes.ROUTE_TARGET, new RouteEndpoint.Local(to))
                .putRaw(ArtemisKinds.ROUTING_TYPE, "STRIP")
                .build();
    }

    private static ConnectRequest connectTo(String password) {
        String host = BROKER.getHost();
        NodeSpec node = new NodeSpec(
                NodeId.of("primary"),
                List.of(
                        Endpoint.of(
                                Plane.MANAGEMENT,
                                "jolokia",
                                "http://" + host + ":" + BROKER.getMappedPort(8161) + "/console/jolokia"),
                        Endpoint.of(Plane.DATA, "core", "tcp://" + host + ":" + BROKER.getMappedPort(61616))));
        return new ConnectRequest(
                "artemis",
                ClusterSpec.ofNodes(List.of(node)),
                CredentialSource.of(Credentials.usernamePassword(USER, password)),
                ConnectOptions.defaults());
    }
}
