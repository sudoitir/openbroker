package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.PlacementRole;
import io.github.sudoitir.openbroker.model.Precision;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.RouteEndpoint;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMq;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqTestCluster;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The resources facet against a real three-node cluster: what it lists is what the broker holds. */
class RabbitMqResourcesIT {

    private static final String VHOST =
            "resources-" + UUID.randomUUID().toString().substring(0, 8);

    private static RabbitMqTestCluster cluster;
    private static RabbitMqCluster connection;
    private static RabbitMqResources resources;

    @BeforeAll
    static void declare() {
        cluster = RabbitMqTestCluster.shared();
        RabbitMq admin = RabbitMq.of(cluster, 0);
        admin.createVhost(VHOST);
        admin.setPermissions(VHOST, RabbitMqTestCluster.ADMIN_USER, ".*", ".*", ".*");
        admin.declareQueue(VHOST, "orders", "classic");
        admin.declareQueue(VHOST, "payments", "quorum");
        admin.declareQueue(VHOST, "audit", "stream");
        admin.declareExchange(VHOST, "orders.events", "fanout");
        admin.bindQueue(VHOST, "orders.events", "orders", "created");
        admin.send(
                "PUT",
                "/api/policies/" + RabbitMq.segment(VHOST) + "/cap",
                Map.of(
                        "pattern",
                        "^orders",
                        "apply-to",
                        "queues",
                        "priority",
                        1,
                        "definition",
                        Map.of("max-length", 100)));
        connection = FakeManagement.cluster(
                cluster.credentials(),
                ConnectOptions.defaults(),
                Endpoint.of(Plane.MANAGEMENT, RabbitMqOptions.HTTP, cluster.managementUrl(0)));
        resources = new RabbitMqResources(connection);
    }

    @AfterAll
    static void close() {
        connection.close();
    }

    private static ResourcePage list(KindId kind) {
        return resources
                .read(ResourceQuery.of(kind).where(Map.of("vhost", VHOST)))
                .orThrow();
    }

    @Test
    void theVirtualHostIsListed() {
        ResourcePage page =
                resources.read(ResourceQuery.of(RabbitMqKinds.VHOST)).orThrow();

        assertThat(page.items()).extracting(r -> r.id().name()).contains("/", VHOST);
    }

    @Test
    void eachQueueIsListedUnderItsOwnKind() {
        assertThat(list(RabbitMqKinds.CLASSIC_QUEUE).items())
                .extracting(r -> r.id().name())
                .containsExactly("orders");
        assertThat(list(RabbitMqKinds.QUORUM_QUEUE).items())
                .extracting(r -> r.id().name())
                .containsExactly("payments");
        assertThat(list(RabbitMqKinds.STREAM).items())
                .extracting(r -> r.id().name())
                .containsExactly("audit");
    }

    @Test
    void aQuorumQueueHasALeaderAndTwoFollowersInSyncOnceSettled() {
        Resource payments = RabbitMq.eventually(
                Duration.ofSeconds(30),
                () -> list(RabbitMqKinds.QUORUM_QUEUE).items().getFirst(),
                r -> r.placements().size() == 3
                        && r.placements().stream().allMatch(p -> p.inSync().orElse(false)));

        assertThat(payments.placements())
                .extracting(Placement::role)
                .containsExactlyInAnyOrder(PlacementRole.LEADER, PlacementRole.FOLLOWER, PlacementRole.FOLLOWER);
        assertThat(payments.attributes().asMap()).containsKey(RabbitMqKinds.LEADER);
    }

    @Test
    void aStreamsBacklogIsAnEstimate() {
        Resource audit = list(RabbitMqKinds.STREAM).items().getFirst();

        assertThat(audit.attributes().get(MetricKeys.BACKLOG).orElseThrow().precision())
                .isEqualTo(Precision.ESTIMATE);
    }

    @Test
    void theBindingNamesItsExchangeAndQueue() {
        Resource binding = list(RabbitMqKinds.BINDING).items().getFirst();

        assertThat(binding.attributes().get(StandardAttributes.FILTER)).contains("created");
        assertThat(((RouteEndpoint.Local) binding.attributes()
                                .get(StandardAttributes.ROUTE_TARGET)
                                .orElseThrow())
                        .resource()
                        .name())
                .isEqualTo("orders");
    }

    @Test
    void thePolicyKeepsItsDefinitionAsText() {
        Resource cap = list(RabbitMqKinds.POLICY).items().getFirst();

        assertThat(cap.attributes().asMap())
                .containsEntry(RabbitMqKinds.PATTERN, "^orders")
                .containsEntry(RabbitMqKinds.DEFINITION_PREFIX + "max-length", "100");
    }

    @Test
    void theAdministratorHasItsPermissionAndIsAUser() {
        assertThat(list(RabbitMqKinds.PERMISSION).items())
                .extracting(r -> r.id().name())
                .contains(RabbitMqTestCluster.ADMIN_USER);
        assertThat(resources
                        .read(ResourceQuery.of(RabbitMqKinds.USER))
                        .orThrow()
                        .items())
                .extracting(r -> r.id().name())
                .contains(RabbitMqTestCluster.ADMIN_USER);
    }

    @Test
    void thePagesOfAServerPagedListingFollowOneAnother() {
        ResourceQuery query = ResourceQuery.of(RabbitMqKinds.CLASSIC_QUEUE)
                .where(Map.of("vhost", VHOST))
                .limit(1);

        ResourcePage page = resources.read(query).orThrow();

        assertThat(page.items()).hasSize(1);
        assertThat(resources
                        .read(ResourceQuery.of(RabbitMqKinds.EXCHANGE).limit(1))
                        .orThrow()
                        .continueToken())
                .isPresent();
    }
}
