package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.Delivery;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.MetricValue;
import io.github.sudoitir.openbroker.model.Ordering;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.Precision;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.RouteClass;
import io.github.sudoitir.openbroker.model.RouteEndpoint;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class RabbitMqResourcesTest {

    private FakeManagement server;
    private RabbitMqResources resources;

    @BeforeEach
    void start() {
        server = new FakeManagement();
        server.json("GET", "/api/vhosts", "vhosts.json")
                .json("GET", "/api/exchanges", "exchanges.json")
                .json("GET", "/api/queues", "queues-detail.json")
                .json("GET", "/api/queues/%2F", "queues-detail.json")
                .json("GET", "/api/bindings", "bindings.json")
                .json("GET", "/api/policies", "policies.json")
                .json("GET", "/api/operator-policies", "operator-policies.json")
                .json("GET", "/api/permissions", "permissions.json")
                .json("GET", "/api/topic-permissions", "topic-permissions.json")
                .json("GET", "/api/users", "users.json")
                .json("GET", "/api/connections", "connections.json")
                .json("GET", "/api/channels", "channels.json")
                .json("GET", "/api/consumers", "consumers.json")
                .json("GET", "/api/parameters/shovel", "shovel-parameters.json")
                .json("GET", "/api/shovels", "shovels.json")
                .json("GET", "/api/federation-links", "federation-links.json")
                .json("GET", "/api/nodes", "nodes-all-plugins.json")
                .json("GET", "/api/overview", "overview.json")
                .json("GET", "/api/vhosts/sales/permissions", "permissions.json")
                .json("GET", "/api/whoami", "whoami.json");
        resources = new RabbitMqResources(FakeManagement.cluster(server.endpoint()));
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private ResourcePage list(KindId kind) {
        return resources.read(ResourceQuery.of(kind)).orThrow();
    }

    private static Resource named(ResourcePage page, String name) {
        return page.items().stream()
                .filter(r -> r.id().name().equals(name))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void theKindsAreTheDriversDeclarations() {
        assertThat(resources.kinds()).isEqualTo(RabbitMqKinds.all());
    }

    @Test
    void vhostsReadTheirDescriptionTagsAndDefaultQueueType() {
        ResourcePage page = list(RabbitMqKinds.VHOST);

        assertThat(page.items()).extracting(r -> r.id().name()).containsExactly("/", "old", "sales");
        Resource sales = named(page, "sales");
        assertThat(sales.id().format()).isEqualTo("rabbitmq.vhost/sales");
        assertThat(sales.attributes().asMap())
                .containsEntry(RabbitMqKinds.DESCRIPTION, "Orders and payments")
                .containsEntry(RabbitMqKinds.TAGS, "production,eu")
                .containsEntry(RabbitMqKinds.DEFAULT_QUEUE_TYPE, "quorum");
        Resource old = named(page, "old");
        assertThat(old.attributes().asMap())
                .containsEntry(RabbitMqKinds.DESCRIPTION, "Older layout")
                .containsEntry(RabbitMqKinds.TAGS, "legacy");
        assertThat(named(page, "/").id().format()).isEqualTo("rabbitmq.vhost/%2F");
    }

    @Test
    void exchangesMapTheDefaultExchangeAndReadArguments() {
        ResourcePage page = list(RabbitMqKinds.EXCHANGE);

        Resource def = named(page, "amq.default");
        assertThat(def.id().path().segments()).containsExactly("/", "amq.default");
        assertThat(def.attributes()
                        .get(StandardAttributes.DELIVERY)
                        .orElseThrow()
                        .modes())
                .containsExactly(Delivery.POINT_TO_POINT);
        Resource events = named(page, "orders.events");
        assertThat(events.attributes().asMap())
                .containsEntry(RabbitMqKinds.EXCHANGE_TYPE, "fanout")
                .containsEntry(RabbitMqKinds.ARGUMENT_PREFIX + "alternate-exchange", "unrouted")
                .containsEntry(RabbitMqKinds.ARGUMENT_PREFIX + "x-max-priority", 5L)
                .containsEntry(RabbitMqKinds.ARGUMENT_PREFIX + "x-lazy", true)
                .containsEntry(RabbitMqKinds.AUTO_DELETE, false);
        assertThat(events.attributes().get(StandardAttributes.PARENT).orElseThrow())
                .isEqualTo(ResourceId.of(RabbitMqKinds.VHOST.value(), "/"));
        assertThat(events.attributes()
                        .get(StandardAttributes.DELIVERY)
                        .orElseThrow()
                        .modes())
                .doesNotContain(Delivery.POINT_TO_POINT);
        assertThat(server.received().getFirst().query())
                .contains("page=1")
                .contains("page_size=500")
                .contains("sort=name");
    }

    @Test
    void queueKindsListOnlyTheirOwnTypeAndSkipUnlistedTypes() {
        assertThat(list(RabbitMqKinds.CLASSIC_QUEUE).items())
                .extracting(r -> r.id().name())
                .containsExactly("orders", "legacy");
        assertThat(list(RabbitMqKinds.QUORUM_QUEUE).items())
                .extracting(r -> r.id().name())
                .containsExactly("payments");
        assertThat(list(RabbitMqKinds.STREAM).items())
                .extracting(r -> r.id().name())
                .containsExactly("audit");
    }

    @Test
    void aQuorumQueueCarriesItsLeaderLimitMetricsAndUnderReplication() {
        Resource payments = named(list(RabbitMqKinds.QUORUM_QUEUE), "payments");

        assertThat(payments.id().format()).isEqualTo("rabbitmq.quorum-queue/%2F/payments");
        assertThat(payments.attributes().asMap())
                .containsEntry(RabbitMqKinds.LEADER, "rabbit@alpha")
                .containsEntry(RabbitMqKinds.DELIVERY_LIMIT, 7L)
                .containsEntry(RabbitMqKinds.EFFECTIVE_POLICY, "payments-limits");
        assertThat(payments.attributes().get(MetricKeys.BACKLOG).orElseThrow().asLong())
                .isEqualTo(40);
        assertThat(payments.placements()).hasSize(3);
        assertThat(payments.conditions())
                .filteredOn(c -> c.type().equals(ConditionTypes.UNDER_REPLICATED))
                .extracting(Condition::status)
                .containsExactly(ConditionStatus.TRUE);
    }

    @Test
    void aStreamsMessageCountsAreEstimatesAndItReadsItsRetention() {
        Resource audit = named(list(RabbitMqKinds.STREAM), "audit");

        MetricValue backlog = audit.attributes().get(MetricKeys.BACKLOG).orElseThrow();
        assertThat(backlog.precision()).isEqualTo(Precision.ESTIMATE);
        assertThat(backlog.asLong()).isEqualTo(1000);
        assertThat(audit.attributes()
                        .get(MetricKeys.CONSUMER_COUNT)
                        .orElseThrow()
                        .precision())
                .isEqualTo(Precision.EXACT);
        assertThat(audit.attributes().asMap())
                .containsEntry(RabbitMqKinds.MAX_AGE, "7D")
                .containsEntry(RabbitMqKinds.MAX_LENGTH_BYTES, 1073741824L)
                .containsEntry(RabbitMqKinds.COMMITTED_OFFSET, 999L)
                .containsEntry(RabbitMqKinds.FIRST_TIMESTAMP, 1761990000000L);
    }

    @Test
    void aClassicQueueReadsDeadLetterTemporaryAndServing() {
        ResourcePage classic = list(RabbitMqKinds.CLASSIC_QUEUE);

        Resource orders = named(classic, "orders");
        assertThat(orders.attributes().get(StandardAttributes.DEAD_LETTER)).contains("dlx");
        assertThat(orders.attributes().get(StandardAttributes.TEMPORARY)).contains(false);
        assertThat(orders.attributes().get(StandardAttributes.PLACEMENT_NODE)).contains("rabbit@alpha");
        Resource legacy = named(classic, "legacy");
        assertThat(legacy.attributes().get(StandardAttributes.TEMPORARY)).contains(true);
        assertThat(legacy.conditions())
                .filteredOn(c -> c.type().equals(ConditionTypes.SERVING))
                .extracting(Condition::status)
                .containsExactly(ConditionStatus.FALSE);
    }

    @Test
    void bindingsLeaveOutTheDefaultExchangeAndNameTheirQueueKind() {
        ResourcePage bindings = list(RabbitMqKinds.BINDING);

        assertThat(bindings.items())
                .extracting(r -> r.id().format())
                .containsExactly(
                        "rabbitmq.binding/%2F/orders.events/orders/created",
                        "rabbitmq.binding/%2F/orders.events/payments/~");
        Resource created = bindings.items().getFirst();
        assertThat(created.attributes().get(StandardAttributes.ROUTE_CLASS)).contains(RouteClass.BINDING);
        assertThat(created.attributes().get(StandardAttributes.FILTER)).contains("created");
        assertThat(created.attributes().asMap()).containsEntry(RabbitMqKinds.ARGUMENT_PREFIX + "x-match", "all");
        assertThat(created.attributes().get(StandardAttributes.ROUTE_SOURCE).orElseThrow())
                .isEqualTo(
                        new RouteEndpoint.Local(ResourceId.of(RabbitMqKinds.EXCHANGE.value(), "/", "orders.events")));
        assertThat(created.attributes().get(StandardAttributes.ROUTE_TARGET).orElseThrow())
                .isEqualTo(new RouteEndpoint.Local(ResourceId.of(RabbitMqKinds.CLASSIC_QUEUE.value(), "/", "orders")));
        assertThat(bindings.items()
                        .get(1)
                        .attributes()
                        .get(StandardAttributes.ROUTE_TARGET)
                        .orElseThrow())
                .isEqualTo(new RouteEndpoint.Local(ResourceId.of(RabbitMqKinds.QUORUM_QUEUE.value(), "/", "payments")));
        assertThat(bindings.items().get(1).attributes().get(StandardAttributes.FILTER))
                .isEmpty();
    }

    @Test
    void exchangeBindingsTargetAnExchange() {
        ResourcePage bindings = list(RabbitMqKinds.EXCHANGE_BINDING);

        assertThat(bindings.items()).hasSize(1);
        Resource binding = bindings.items().getFirst();
        assertThat(binding.id().format()).isEqualTo("rabbitmq.exchange-binding/%2F/amq.topic/orders.events/orders.%23");
        assertThat(binding.attributes().get(StandardAttributes.ROUTE_TARGET).orElseThrow())
                .isEqualTo(
                        new RouteEndpoint.Local(ResourceId.of(RabbitMqKinds.EXCHANGE.value(), "/", "orders.events")));
    }

    @Test
    void policiesAndOperatorPoliciesReadTheirDefinitionAsText() {
        Resource ha = named(list(RabbitMqKinds.POLICY), "ha");

        assertThat(ha.id().format()).isEqualTo("rabbitmq.policy/%2F/ha");
        assertThat(ha.attributes().asMap())
                .containsEntry(RabbitMqKinds.PATTERN, "^payments")
                .containsEntry(RabbitMqKinds.APPLY_TO, "queues")
                .containsEntry(RabbitMqKinds.PRIORITY, 5L)
                .containsEntry(RabbitMqKinds.DEFINITION_PREFIX + "max-length", "10000")
                .containsEntry(RabbitMqKinds.DEFINITION_PREFIX + "overflow", "\"reject-publish\"");
        assertThat(list(RabbitMqKinds.OPERATOR_POLICY).items())
                .extracting(r -> r.id().format())
                .containsExactly("rabbitmq.operator-policy/%2F/cap");
    }

    @Test
    void permissionsAndTopicPermissionsReadTheirPatterns() {
        ResourcePage permissions = list(RabbitMqKinds.PERMISSION);

        assertThat(permissions.items()).hasSize(3);
        Resource app = permissions.items().stream()
                .filter(r -> r.id().path().segments().equals(List.of("/", "app")))
                .findFirst()
                .orElseThrow();
        assertThat(app.attributes().asMap())
                .containsEntry(RabbitMqKinds.WRITE, "^orders")
                .doesNotContainKey(RabbitMqKinds.CONFIGURE)
                .containsEntry(StandardAttributes.PRINCIPAL.name(), "app");
        Resource topic = list(RabbitMqKinds.TOPIC_PERMISSION).items().getFirst();
        assertThat(topic.id().path().segments()).containsExactly("/", "app", "amq.topic");
        assertThat(topic.attributes().asMap())
                .containsEntry(RabbitMqKinds.READ, "^orders\\.")
                .doesNotContainKey(RabbitMqKinds.CONFIGURE);
    }

    @Test
    void usersReadTagsAndNeverThePasswordHash() {
        ResourcePage users = list(RabbitMqKinds.USER);

        assertThat(named(users, "ops").attributes().asMap()).containsEntry(RabbitMqKinds.TAGS, "administrator");
        assertThat(named(users, "app").attributes().asMap()).containsEntry(RabbitMqKinds.TAGS, "monitoring,management");
        assertThat(users.items().toString()).doesNotContain("c2VjcmV0aGFzaA").doesNotContain("password");
    }

    @Test
    void connectionsReadWhoAndWhereTheyAre() {
        ResourcePage connections = list(RabbitMqKinds.CONNECTION);

        Resource first = named(connections, "10.0.0.5:52114 -> 10.0.0.1:5672");
        assertThat(first.attributes().get(StandardAttributes.CLIENT_ID)).contains("order-service");
        assertThat(first.attributes().get(StandardAttributes.PRINCIPAL)).contains("app");
        assertThat(first.attributes().get(StandardAttributes.REMOTE_ADDRESS)).contains("10.0.0.5:52114");
        assertThat(first.attributes().get(StandardAttributes.PROTOCOL)).contains("AMQP 0-9-1");
        assertThat(first.attributes()
                        .get(StandardAttributes.CREATED_AT)
                        .orElseThrow()
                        .toEpochMilli())
                .isEqualTo(1761990000000L);
        assertThat(first.attributes().asMap())
                .containsEntry(RabbitMqKinds.CHANNELS, 2L)
                .containsEntry(RabbitMqKinds.VHOST_NAME, "/");
        assertThat(first.placements().getFirst().node()).isEqualTo(NodeId.of("rabbit@alpha"));
        assertThat(connections.items().get(1).attributes().get(StandardAttributes.CLIENT_ID))
                .isEmpty();
    }

    @Test
    void channelsPointToTheirConnection() {
        Resource channel = list(RabbitMqKinds.CHANNEL).items().getFirst();

        assertThat(channel.attributes().get(StandardAttributes.CONNECTION).orElseThrow())
                .isEqualTo(ResourceId.of(RabbitMqKinds.CONNECTION.value(), "10.0.0.5:52114 -> 10.0.0.1:5672"));
        assertThat(channel.attributes().asMap())
                .containsEntry(RabbitMqKinds.PREFETCH_COUNT, 20L)
                .containsEntry(RabbitMqKinds.UNACKED, 3L)
                .containsEntry(RabbitMqKinds.CONFIRM, true)
                .containsEntry(RabbitMqKinds.TRANSACTIONAL, false);
        assertThat(channel.attributes().get(StandardAttributes.CONSUMER_COUNT)).contains(2L);
    }

    @Test
    void consumersNameTheirQueueByKindAndTheirChannel() {
        ResourcePage consumers = list(RabbitMqKinds.CONSUMER);

        Resource first = named(consumers, "amq.ctag-AbC");
        assertThat(first.id().path().segments())
                .containsExactly("/", "10.0.0.5:52114 -> 10.0.0.1:5672 (1)", "amq.ctag-AbC");
        assertThat(first.attributes().get(StandardAttributes.SUBSCRIPTION).orElseThrow())
                .isEqualTo(ResourceId.of(RabbitMqKinds.CLASSIC_QUEUE.value(), "/", "orders"));
        assertThat(first.attributes().get(StandardAttributes.CHANNEL)).isPresent();
        assertThat(first.attributes().asMap())
                .containsEntry(RabbitMqKinds.ACK_REQUIRED, true)
                .containsEntry(RabbitMqKinds.ACTIVE, true)
                .containsEntry(RabbitMqKinds.EXCLUSIVE, false);
        assertThat(named(consumers, "pay-1")
                        .attributes()
                        .get(StandardAttributes.SUBSCRIPTION)
                        .orElseThrow())
                .isEqualTo(ResourceId.of(RabbitMqKinds.QUORUM_QUEUE.value(), "/", "payments"));
    }

    @Test
    void shovelsMergeTheirParameterWithTheirStatusAndRedactRemoteAddresses() {
        ResourcePage shovels = list(RabbitMqKinds.SHOVEL);

        assertThat(shovels.items()).extracting(r -> r.id().name()).containsExactly("static-copy", "stopped", "to-dr");
        Resource toDr = named(shovels, "to-dr");
        assertThat(toDr.attributes().get(StandardAttributes.ROUTE_CLASS)).contains(RouteClass.FORWARD);
        assertThat(toDr.attributes().get(StandardAttributes.ROUTE_SOURCE).orElseThrow())
                .isInstanceOf(RouteEndpoint.Local.class);
        RouteEndpoint.Remote target = (RouteEndpoint.Remote)
                toDr.attributes().get(StandardAttributes.ROUTE_TARGET).orElseThrow();
        assertThat(target.uri()).isEqualTo(URI.create("amqps://dr.example.com:5671/dr"));
        assertThat(target.name()).isEqualTo("orders.in");
        assertThat(toDr.conditions()).extracting(Condition::status).containsExactly(ConditionStatus.TRUE);
        assertThat(toDr.attributes().get(StandardAttributes.MANAGED_BY)).isEmpty();
        assertThat(toDr.toString()).doesNotContain("topsecret").doesNotContain("cacertfile");
        Resource stopped = named(shovels, "stopped");
        assertThat(stopped.conditions()).isEmpty();
        assertThat(stopped.attributes().get(StandardAttributes.ROUTE_SOURCE).orElseThrow())
                .isEqualTo(new RouteEndpoint.Local(
                        ResourceId.of(RabbitMqKinds.EXCHANGE.value(), "sales", "orders.events")));
    }

    @Test
    void aStaticShovelIsManagedByConfiguration() {
        Resource copy = named(list(RabbitMqKinds.SHOVEL), "static-copy");

        assertThat(copy.attributes().get(StandardAttributes.MANAGED_BY)).contains("configuration");
        assertThat(copy.conditions()).extracting(Condition::status).containsExactly(ConditionStatus.FALSE);
        assertThat(copy.placements().getFirst().node()).isEqualTo(NodeId.of("rabbit@beta"));
        assertThat(((RouteEndpoint.Remote) copy.attributes()
                                .get(StandardAttributes.ROUTE_TARGET)
                                .orElseThrow())
                        .name())
                .isEqualTo("audit-copy");
    }

    @Test
    void federationLinksReadTheirUpstreamAndPolicy() {
        ResourcePage links = list(RabbitMqKinds.FEDERATION_LINK);

        assertThat(links.items()).hasSize(2);
        Resource exchange = links.items().stream()
                .filter(r -> r.id().path().segments().contains("exchange"))
                .findFirst()
                .orElseThrow();
        assertThat(exchange.id().path().segments()).containsExactly("/", "eu", "exchange", "orders.events");
        assertThat(exchange.attributes().get(StandardAttributes.ROUTE_CLASS)).contains(RouteClass.LINK);
        assertThat(exchange.attributes().get(StandardAttributes.DERIVED_FROM))
                .contains("policy federate-orders, upstream eu");
        RouteEndpoint.Remote source = (RouteEndpoint.Remote)
                exchange.attributes().get(StandardAttributes.ROUTE_SOURCE).orElseThrow();
        assertThat(source.uri()).isEqualTo(URI.create("amqps://eu.example.com:5671/%2F"));
        assertThat(source.name()).isEqualTo("orders.events");
        assertThat(exchange.conditions().getFirst().status()).isEqualTo(ConditionStatus.TRUE);
        Resource queue = links.items().stream()
                .filter(r -> r.id().path().segments().contains("queue"))
                .findFirst()
                .orElseThrow();
        assertThat(queue.attributes().get(StandardAttributes.DERIVED_FROM)).contains("upstream us");
        assertThat(queue.conditions().getFirst().status()).isEqualTo(ConditionStatus.FALSE);
    }

    @Test
    void federationLinksAreEmptyWithoutThePlugin() {
        try (FakeManagement bareServer = new FakeManagement()
                .json("GET", "/api/nodes", "nodes-no-plugins.json")
                .json("GET", "/api/federation-links", "federation-links.json")) {
            RabbitMqResources bare = new RabbitMqResources(FakeManagement.cluster(bareServer.endpoint()));

            assertThat(bare.read(ResourceQuery.of(RabbitMqKinds.FEDERATION_LINK))
                            .orThrow()
                            .items())
                    .isEmpty();
        }
    }

    @Test
    void aClassQueryMergesItsKindsByNameWithAnOffsetToken() {
        ResourceQuery query = ResourceQuery.of(ResourceClass.SUBSCRIPTION).limit(2);

        ResourcePage first = resources.read(query).orThrow();
        assertThat(first.items()).extracting(r -> r.id().name()).containsExactly("audit", "legacy");
        assertThat(first.continueToken()).isPresent();
        ResourcePage second =
                resources.read(query.continueFrom(first.continueToken().get())).orThrow();
        assertThat(second.items()).extracting(r -> r.id().name()).containsExactly("orders", "payments");
        assertThat(second.continueToken()).isEmpty();
    }

    @Test
    void aDescendingNameOrderingReversesAClientListing() {
        ResourcePage page = resources
                .read(ResourceQuery.of(RabbitMqKinds.VHOST).orderBy(List.of(Ordering.desc("name"))))
                .orThrow();

        assertThat(page.items()).extracting(r -> r.id().name()).containsExactly("sales", "old", "/");
    }

    @Test
    void clientPagingCutsAtTheLimitAndResumesFromTheToken() {
        ResourceQuery query = ResourceQuery.of(RabbitMqKinds.VHOST).limit(2);

        ResourcePage first = resources.read(query).orThrow();
        assertThat(first.items()).hasSize(2);
        String token = first.continueToken().orElseThrow();
        assertThat(Listing.decode(Optional.of(token)).offset()).isEqualTo(2);
        ResourcePage second = resources.read(query.continueFrom(token)).orThrow();
        assertThat(second.items()).extracting(r -> r.id().name()).containsExactly("sales");
        assertThat(second.continueToken()).isEmpty();
    }

    @Test
    void serverPagingAsksForOnePageAndHandsBackTheNext() {
        FakeManagement paged = new FakeManagement()
                .json("GET", "/api/queues", "queues-page-1.json")
                .json("GET", "/api/queues", "queues-page-2.json")
                .json("GET", "/api/queues", "queues-page-3.json");
        try (paged) {
            RabbitMqResources paging = new RabbitMqResources(FakeManagement.cluster(paged.endpoint()));
            ResourceQuery query = ResourceQuery.of(RabbitMqKinds.CLASSIC_QUEUE).limit(2);

            ResourcePage first = paging.read(query).orThrow();
            assertThat(first.items()).extracting(r -> r.id().name()).containsExactly("a", "b");
            assertThat(Listing.decode(first.continueToken()).page()).isEqualTo(2);
            assertThat(Listing.decode(first.continueToken()).server()).isTrue();
            assertThat(paged.received().getFirst().query()).contains("page=1").contains("page_size=2");

            // The second page holds only quorum queues: nothing for this kind, so the next page is read.
            ResourcePage second =
                    paging.read(query.continueFrom(first.continueToken().get())).orThrow();
            assertThat(second.items()).extracting(r -> r.id().name()).containsExactly("e");
            assertThat(second.continueToken()).isEmpty();
            assertThat(paged.received()).hasSize(3);
            assertThat(paged.received().get(2).query()).contains("page=3");
        }
    }

    @Test
    void aNoLimitQueryReadsEveryServerPage() {
        FakeManagement paged = new FakeManagement()
                .json("GET", "/api/queues", "queues-page-1.json")
                .json("GET", "/api/queues", "queues-page-2.json")
                .json("GET", "/api/queues", "queues-page-3.json");
        try (paged) {
            RabbitMqResources paging = new RabbitMqResources(FakeManagement.cluster(paged.endpoint()));

            ResourcePage all =
                    paging.read(ResourceQuery.of(RabbitMqKinds.QUORUM_QUEUE)).orThrow();

            assertThat(all.items()).extracting(r -> r.id().name()).containsExactly("c", "d");
            assertThat(all.continueToken()).isEmpty();
            assertThat(paged.received()).hasSize(3);
        }
    }

    @Test
    void aNamePatternBecomesARegexForTheServerAndAMatchForTheClient() {
        ResourcePage queues = resources
                .read(ResourceQuery.of(RabbitMqKinds.CLASSIC_QUEUE).named("ord*"))
                .orThrow();
        ResourcePage vhosts = resources
                .read(ResourceQuery.of(RabbitMqKinds.VHOST).named("s?les"))
                .orThrow();

        assertThat(queues.items()).extracting(r -> r.id().name()).contains("orders");
        String query = server.received().getFirst().query();
        assertThat(query).contains("use_regex=true").contains("name=%5Eord.%2A%24");
        assertThat(vhosts.items()).extracting(r -> r.id().name()).containsExactly("sales");
        assertThat(RabbitMqResources.regex("a.b*?")).isEqualTo("^a\\.b.*.$");
    }

    @Test
    void aVhostSelectorNarrowsThePathAndFiltersTheItems() {
        ResourcePage page = resources
                .read(ResourceQuery.of(RabbitMqKinds.QUORUM_QUEUE).where(Map.of("vhost", "/")))
                .orThrow();

        assertThat(page.items()).extracting(r -> r.id().name()).containsExactly("payments");
        assertThat(server.received().getFirst().path()).isEqualTo("/api/queues/%2F");
        assertThat(resources
                        .read(ResourceQuery.of(RabbitMqKinds.PERMISSION).where(Map.of("vhost", "sales")))
                        .orThrow()
                        .items())
                .extracting(r -> r.id().name())
                .containsExactly("app");
    }

    @Test
    void aProjectionKeepsTheNamedAttributesAndReducesTheQueueStatistics() {
        ResourcePage page = resources
                .read(ResourceQuery.of(RabbitMqKinds.QUORUM_QUEUE).project(Set.of("openbroker.backlog")))
                .orThrow();

        assertThat(page.items().getFirst().attributes().asMap()).containsOnlyKeys("openbroker.backlog");
        assertThat(page.items().getFirst().placements()).hasSize(3);
        assertThat(server.received().getFirst().query())
                .contains("disable_stats=true")
                .contains("enable_queue_totals=true");
    }

    @Test
    void aStreamIsListedWithItsStatisticsWhateverTheProjection() {
        resources
                .read(ResourceQuery.of(RabbitMqKinds.STREAM)
                        .project(Set.of("openbroker.backlog", RabbitMqKinds.COMMITTED_OFFSET)))
                .orThrow();

        assertThat(server.received().getFirst().query()).doesNotContain("disable_stats");
    }

    @Test
    void aNodeScopeKeepsTheResourcesPlacedOnThatNode() {
        ResourcePage page = resources
                .read(ResourceQuery.of(RabbitMqKinds.QUORUM_QUEUE).in(Scope.node(NodeId.of("rabbit@gamma"))))
                .orThrow();
        ResourcePage none = resources
                .read(ResourceQuery.of(RabbitMqKinds.QUORUM_QUEUE).in(Scope.node(NodeId.of("rabbit@delta"))))
                .orThrow();

        assertThat(page.items()).hasSize(1);
        assertThat(none.items()).isEmpty();
    }

    @Test
    void invalidQueriesFailBeforeAnyRequest() {
        assertInvalid(ResourceQuery.of(RabbitMqKinds.VHOST).orderBy(List.of(Ordering.asc("backlog"))));
        assertInvalid(ResourceQuery.of(RabbitMqKinds.VHOST).where(Map.of("colour", "red")));
        assertInvalid(ResourceQuery.of(KindId.of("kafka.topic")));
        assertInvalid(ResourceQuery.of(ResourceClass.PRODUCER));
        assertInvalid(ResourceQuery.of(RabbitMqKinds.VHOST).continueFrom("not-a-token"));
        assertInvalid(
                ResourceQuery.of(RabbitMqKinds.VHOST).in(Scope.shards(Set.of(Shard.partition(PartitionId.of("0"))))));
        assertInvalid(
                ResourceQuery.of(RabbitMqKinds.EXCHANGE).continueFrom(Listing.token(new Listing.Cursor(false, 1))));
        assertInvalid(
                ResourceQuery.of(ResourceClass.SUBSCRIPTION).continueFrom(Listing.token(new Listing.Cursor(true, 1))));
        assertThat(server.received()).isEmpty();
    }

    private void assertInvalid(ResourceQuery query) {
        Partial<ResourcePage> result = resources.read(query);
        assertThat(result.isComplete()).isFalse();
        assertThat(result.value().items()).isEmpty();
        assertThatThrownBy(result::orThrow)
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }

    @Test
    void oneFailingQueryDoesNotFailTheOthers() {
        Partial<List<ResourcePage>> result;
        try (FakeManagement partial = new FakeManagement()
                .respond("GET", "/api/users", 403, Map.of(), "{\"error\":\"not_authorised\",\"reason\":\"no\"}")
                .json("GET", "/api/vhosts", "vhosts.json")) {
            result = new RabbitMqResources(FakeManagement.cluster(partial.endpoint()))
                    .read(List.of(ResourceQuery.of(RabbitMqKinds.USER), ResourceQuery.of(RabbitMqKinds.VHOST)));
        }

        assertThat(result.value().get(0).items()).isEmpty();
        assertThat(result.value().get(1).items()).hasSize(3);
        assertThat(result.failures()).containsKey(RabbitMqCluster.CLUSTER);
        assertThat(result.failures().get(RabbitMqCluster.CLUSTER).status()).isEqualTo(Status.PERMISSION_DENIED);
    }

    @Test
    void aListingThatIsNotAListIsABadResponse() {
        try (FakeManagement odd = new FakeManagement()
                .respond("GET", "/api/policies", 200, Map.of("Content-Type", "application/json"), "{}")) {
            RabbitMqResources reading = new RabbitMqResources(FakeManagement.cluster(odd.endpoint()));

            ResourceQuery policies = ResourceQuery.of(RabbitMqKinds.POLICY);
            assertThatThrownBy(() -> reading.read(policies).orThrow()).isInstanceOf(BrokerException.class);
        }
    }

    @Test
    void aQueueOfAnUnlistedTypeIsRefusedWhenReadDirectly() {
        Queues queues = new Queues(FakeManagement.cluster(server.endpoint()));
        JsonNode json = new JsonMapper().readTree("{\"name\":\"m\",\"vhost\":\"/\",\"type\":\"mqtt-qos0\"}");

        assertThatThrownBy(() -> queues.read(json)).isInstanceOf(BrokerException.class);
    }

    @Test
    void remoteAddressesLoseTheirUserInformationQueryAndMalformedTextBecomesAPlaceholder() {
        assertThat(ReadSupport.redacted("amqps://u:p@h.example.com:5671/v?password=x#f"))
                .isEqualTo(URI.create("amqps://h.example.com:5671/v"));
        assertThat(ReadSupport.redacted("not a uri")).isEqualTo(URI.create("amqp://unknown"));
        assertThat(ReadSupport.redacted("amqp://bad host/x")).isEqualTo(URI.create("amqp://unknown"));
        assertThat(ReadSupport.isLocal("amqp://")).isTrue();
        assertThat(ReadSupport.isLocal("amqp:///sales")).isTrue();
        assertThat(ReadSupport.isLocal("amqp://host")).isFalse();
        assertThat(ReadSupport.localVhost("amqp:///my%2Fhost", "x")).isEqualTo("my/host");
        assertThat(ReadSupport.localVhost("amqp://", "x")).isEqualTo("x");
    }

    @Test
    void aMetricsValueIsNotInventedWhenTheListingOmitsIt() {
        Resource bare;
        try (FakeManagement odd = new FakeManagement()
                .respond(
                        "GET",
                        "/api/queues",
                        200,
                        Map.of("Content-Type", "application/json"),
                        "{\"items\":[{\"name\":\"bare\",\"vhost\":\"/\",\"arguments\":{}}],\"page\":1,\"page_count\":1}")) {
            bare = new RabbitMqResources(FakeManagement.cluster(odd.endpoint()))
                    .read(ResourceQuery.of(RabbitMqKinds.CLASSIC_QUEUE))
                    .orThrow()
                    .items()
                    .getFirst();
        }

        assertThat(bare.attributes().get(MetricKeys.BACKLOG)).isEmpty();
        assertThat(bare.placements()).isEmpty();
        assertThat(bare.conditions()).isEmpty();
    }
}
