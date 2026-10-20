package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.Delivery;
import io.github.sudoitir.openbroker.model.DeliveryModes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.MetricValue;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.RouteClass;
import io.github.sudoitir.openbroker.model.RouteEndpoint;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Rows as the measured brokers answer them, read into resources. */
class RowsTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final NodeId NODE = NodeId.of("n1");
    private static final Instant NOW = Instant.parse("2026-10-20T00:00:00Z");

    private static List<Resource> rows(KindId kind, String fixture) {
        JsonNode envelope = JSON.readTree(FixtureServer.fixture(fixture));
        JsonNode value = envelope.get("value");
        JsonNode list = value.isString() ? JSON.readTree(value.asString()) : value;
        return ListRows.parse(kind, list.get("data"), NODE, NOW);
    }

    private static ResourceId id(KindId kind, String... segments) {
        return new ResourceId(kind, ResourcePath.of(segments));
    }

    @Test
    void aQueueRowCarriesItsAddressMetricsAndRoutingTypeAndSkipsInternalQueues() {
        List<Resource> queues = rows(ArtemisKinds.QUEUE, "list-queues.json");

        Resource dlq = queues.stream()
                .filter(r -> r.id().name().equals("DLQ"))
                .findFirst()
                .orElseThrow();
        assertThat(dlq.attributes().get(StandardAttributes.DESTINATION)).contains(id(ArtemisKinds.ADDRESS, "DLQ"));
        assertThat(dlq.attributes().get(StandardAttributes.DURABLE)).contains(true);
        assertThat(dlq.attributes().raw(ArtemisKinds.ROUTING_TYPE)).contains("ANYCAST");
        assertThat(dlq.attributes().get(MetricKeys.BACKLOG)).contains(MetricValue.exact(0));
        assertThat(dlq.placements())
                .singleElement()
                .satisfies(p -> assertThat(p.node()).isEqualTo(NODE));
        assertThat(queues).noneMatch(r -> r.id().name().startsWith("$sys"));
    }

    @Test
    void anAddressRowCountsItsSubscriptions() {
        Resource address = rows(ArtemisKinds.ADDRESS, "list-addresses.json").getFirst();

        assertThat(address.id()).isEqualTo(id(ArtemisKinds.ADDRESS, "orders.in"));
        assertThat(address.attributes().raw(ArtemisKinds.ROUTING_TYPES)).contains("ANYCAST");
        assertThat(address.attributes().get(MetricKeys.SUBSCRIPTION_COUNT)).contains(MetricValue.exact(1));
        assertThat(address.attributes().get(StandardAttributes.DELIVERY))
                .contains(DeliveryModes.of(Delivery.POINT_TO_POINT));
    }

    @Test
    void aConnectionRowNamesItsClientAndParsesItsCreationTime() {
        Resource connection =
                rows(ArtemisKinds.CONNECTION, "list-connections.json").getFirst();

        assertThat(connection.id()).isEqualTo(id(ArtemisKinds.CONNECTION, "8b321c8c"));
        assertThat(connection.attributes().get(StandardAttributes.PROTOCOL)).contains("CORE");
        assertThat(connection.attributes().get(StandardAttributes.REMOTE_ADDRESS))
                .contains("172.21.0.5:53156");
        assertThat(connection.attributes().get(StandardAttributes.CLIENT_ID)).isEmpty();
        assertThat(connection.attributes().get(StandardAttributes.CREATED_AT))
                .contains(Instant.parse("2026-09-03T13:09:06Z"));
    }

    @Test
    void aSessionBelongsToItsConnection() {
        Resource session = rows(ArtemisKinds.SESSION, "list-sessions.json").getFirst();

        assertThat(session.attributes().get(StandardAttributes.PARENT))
                .contains(id(ArtemisKinds.CONNECTION, "7762766a"));
        assertThat(session.attributes().get(StandardAttributes.PRINCIPAL)).contains("artemis");
    }

    @Test
    void aConsumerIsNamedWithinItsSessionAndReadsItsQueue() {
        Resource consumer = rows(ArtemisKinds.CONSUMER, "list-consumers.json").getFirst();

        assertThat(consumer.id()).isEqualTo(id(ArtemisKinds.CONSUMER, "dbb33b79-a798-11f1-985f-0e2f3e17310e", "1849"));
        assertThat(consumer.attributes().get(StandardAttributes.SUBSCRIPTION))
                .contains(id(ArtemisKinds.QUEUE, "SPIKE.A.q000"));
        assertThat(consumer.attributes().get(StandardAttributes.DESTINATION))
                .contains(id(ArtemisKinds.ADDRESS, "SPIKE.A"));
        assertThat(consumer.attributes().raw(ArtemisKinds.STATUS)).contains("OK");
    }

    @Test
    void aProducerReadsEpochMillisForItsCreationTime() {
        Resource producer = rows(ArtemisKinds.PRODUCER, "list-producers.json").getFirst();

        assertThat(producer.attributes().get(StandardAttributes.CREATED_AT))
                .contains(Instant.ofEpochMilli(1788441038852L));
        assertThat(producer.attributes().get(MetricKeys.SENT)).contains(MetricValue.exact(0));
    }

    @Test
    void aDivertForwardsBetweenTwoAddresses() {
        Resource divert = RouteRows.divert(
                JSON.readTree("{\"UniqueName\":\"d1\",\"Address\":\"in\",\"ForwardingAddress\":\"out\","
                        + "\"Filter\":\"color='red'\",\"Exclusive\":true}"),
                NODE,
                NOW);

        assertThat(divert.attributes().get(StandardAttributes.ROUTE_CLASS)).contains(RouteClass.FORWARD);
        assertThat(divert.attributes().get(StandardAttributes.ROUTE_SOURCE))
                .contains(new RouteEndpoint.Local(id(ArtemisKinds.ADDRESS, "in")));
        assertThat(divert.attributes().get(StandardAttributes.ROUTE_TARGET))
                .contains(new RouteEndpoint.Local(id(ArtemisKinds.ADDRESS, "out")));
        assertThat(divert.attributes().raw(StandardAttributes.FILTER.name())).contains("color='red'");
    }

    @Test
    void aBridgeLinksAQueueToARemoteConnectorAndStatesItsHealth() {
        Resource bridge = RouteRows.bridge(
                JSON.readTree("{\"Name\":\"b1\",\"QueueName\":\"q\",\"ForwardingAddress\":\"far\","
                        + "\"StaticConnectors\":[\"backup connector\"],\"Started\":true,\"Connected\":false}"),
                NODE,
                NOW);

        assertThat(bridge.attributes().get(StandardAttributes.ROUTE_TARGET))
                .contains(new RouteEndpoint.Remote(URI.create("urn:artemis:connector:backup+connector"), "far"));
        assertThat(bridge.conditions())
                .extracting(c -> c.type() + "=" + c.status())
                .containsExactly(
                        ConditionTypes.STARTED + "=" + ConditionStatus.TRUE,
                        ConditionTypes.CONNECTED + "=" + ConditionStatus.FALSE);
    }

    @Test
    void routingTypesAreDeliveryModes() {
        assertThat(ListRows.delivery("ANYCAST,MULTICAST"))
                .contains(DeliveryModes.of(Delivery.POINT_TO_POINT, Delivery.PUBLISH_SUBSCRIBE));
        assertThat(ListRows.delivery("")).isEmpty();
    }

    @Test
    void aDeadLetteredMessageNamesWhereItWasFirstSent() {
        Message m = BrowseRows.parse(
                JSON.readTree("{\"messageID\":7,\"StringProperties\":{\"_AMQ_ORIG_ADDRESS\":\"orders\",\"k\":\"v\"}}"),
                new ResourceId(ArtemisKinds.QUEUE, ResourcePath.of("DLQ")),
                NODE,
                -1,
                1024);

        assertThat(m.headers().origin()).contains("orders");
        assertThat(m.properties()).containsOnlyKeys("k");
    }

    @Test
    void anUnreadableCreationTimeIsLeftOut() {
        assertThat(ListRows.createdAt("yesterday")).isNull();
        assertThat(ListRows.createdAt(null)).isNull();
    }
}
