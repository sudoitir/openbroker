package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.model.AttributeKey;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

class EventMappingTest {

    private static final Instant RECEIVED = Instant.parse("2026-01-01T00:00:00Z");
    private static final BiFunction<String, String, KindId> QUORUM = (vhost, queue) -> RabbitMqKinds.QUORUM_QUEUE;

    private static Map<String, Object> headers(Object... pairs) {
        Map<String, Object> out = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put((String) pairs[i], pairs[i + 1]);
        }
        return out;
    }

    private static BrokerEvent map(String key, Map<String, Object> headers) {
        return EventMapping.map(key, headers, RECEIVED, 7, QUORUM).orElseThrow();
    }

    private static ResourceId id(KindId kind, String... path) {
        return new ResourceId(kind, ResourcePath.of(path));
    }

    @Test
    void aQueueCreatedNamesItsKindFromItsArguments() {
        BrokerEvent event = map(
                "queue.created",
                headers("name", "orders", "vhost", "/", "arguments", Map.of("x-queue-type", "stream")));
        assertThat(event.type()).isEqualTo(EventTypes.RESOURCE_CREATED);
        assertThat(event.subject()).contains(id(RabbitMqKinds.STREAM, "/", "orders"));
        assertThat(event.source()).isEqualTo(RabbitMqCluster.CLUSTER);
    }

    @Test
    void aQueueWhoseArgumentsNameNoTypeTakesTheKindTheResolverFinds() {
        BrokerEvent deleted = map("queue.deleted", headers("name", "orders", "vhost", "eu"));
        assertThat(deleted.type()).isEqualTo(EventTypes.RESOURCE_DELETED);
        assertThat(deleted.subject()).contains(id(RabbitMqKinds.QUORUM_QUEUE, "eu", "orders"));
        BrokerEvent classic = map(
                "queue.created", headers("name", "q", "vhost", "/", "arguments", Map.of("x-queue-type", "classic")));
        assertThat(classic.subject()).contains(id(RabbitMqKinds.CLASSIC_QUEUE, "/", "q"));
        BrokerEvent quorum =
                map("queue.created", headers("name", "q", "vhost", "/", "arguments", Map.of("x-queue-type", "quorum")));
        assertThat(quorum.subject()).contains(id(RabbitMqKinds.QUORUM_QUEUE, "/", "q"));
        BrokerEvent stream = map("queue.created", headers("name", "q", "vhost", "/", "type", "rabbit_stream_queue"));
        assertThat(stream.subject()).contains(id(RabbitMqKinds.STREAM, "/", "q"));
        BrokerEvent odd =
                map("queue.created", headers("name", "q", "vhost", "/", "arguments", Map.of("x-queue-type", "other")));
        assertThat(odd.subject()).contains(id(RabbitMqKinds.QUORUM_QUEUE, "/", "q"));
    }

    @Test
    void theTwoArgumentFormTakesQueuesForClassicQueues() {
        BrokerEvent event = EventMapping.map("queue.created", headers("name", "orders", "vhost", "/"))
                .orElseThrow();
        assertThat(event.subject()).contains(id(RabbitMqKinds.CLASSIC_QUEUE, "/", "orders"));
    }

    @Test
    void exchangesVhostsPoliciesPermissionsUsersAndShovelsAreCreatedAndDeleted() {
        assertThat(map("exchange.created", headers("name", "logs", "vhost", "/"))
                        .subject())
                .contains(id(RabbitMqKinds.EXCHANGE, "/", "logs"));
        assertThat(map("exchange.deleted", headers("name", "", "vhost", "/")).subject())
                .contains(id(RabbitMqKinds.EXCHANGE, "/", "amq.default"));
        assertThat(map("vhost.created", headers("name", "eu")).subject()).contains(id(RabbitMqKinds.VHOST, "eu"));
        assertThat(map("vhost.deleted", headers("name", "eu")).type()).isEqualTo(EventTypes.RESOURCE_DELETED);
        BrokerEvent set = map("policy.set", headers("name", "ttl", "vhost", "/"));
        assertThat(set.type()).isEqualTo(EventTypes.RESOURCE_CREATED);
        assertThat(set.subject()).contains(id(RabbitMqKinds.POLICY, "/", "ttl"));
        assertThat(map("policy.cleared", headers("name", "ttl", "vhost", "/")).type())
                .isEqualTo(EventTypes.RESOURCE_DELETED);
        assertThat(map("permission.created", headers("user", "ann", "vhost", "/"))
                        .subject())
                .contains(id(RabbitMqKinds.PERMISSION, "/", "ann"));
        assertThat(map("permission.deleted", headers("user", "ann", "vhost", "/"))
                        .type())
                .isEqualTo(EventTypes.RESOURCE_DELETED);
        assertThat(map("user.created", headers("name", "ann")).subject()).contains(id(RabbitMqKinds.USER, "ann"));
        assertThat(map("user.deleted", headers("name", "ann")).type()).isEqualTo(EventTypes.RESOURCE_DELETED);
        BrokerEvent shovel = map("parameter.set", headers("component", "shovel", "name", "s1", "vhost", "/"));
        assertThat(shovel.type()).isEqualTo(EventTypes.RESOURCE_CREATED);
        assertThat(shovel.subject()).contains(id(RabbitMqKinds.SHOVEL, "/", "s1"));
        assertThat(map("parameter.cleared", headers("component", "shovel", "name", "s1", "vhost", "/"))
                        .type())
                .isEqualTo(EventTypes.RESOURCE_DELETED);
    }

    @Test
    void aParameterOfAnotherComponentIsCarriedUnderTheDriversNamespace() {
        BrokerEvent event = map(
                "parameter.set",
                headers("component", "federation-upstream", "name", "u", "vhost", "/", "value", "amqp://a:secret@h"));
        assertThat(event.type()).isEqualTo("rabbitmq.parameter.set");
        assertThat(event.subject()).isEmpty();
        assertThat(event.data().raw("rabbitmq.component")).contains("federation-upstream");
        assertThat(event.data().asMap().values()).noneMatch(v -> v.toString().contains("secret"));
    }

    @Test
    void bindingsAreCreatedAndDeletedAndDefaultExchangeBindingsAreLeftOut() {
        BrokerEvent queueBinding = map(
                "binding.created",
                headers(
                        "source_name",
                        "logs",
                        "destination_name",
                        "audit",
                        "destination_kind",
                        "queue",
                        "routing_key",
                        "info",
                        "vhost",
                        "/"));
        assertThat(queueBinding.type()).isEqualTo(EventTypes.RESOURCE_CREATED);
        assertThat(queueBinding.subject()).contains(id(RabbitMqKinds.BINDING, "/", "logs", "audit", "info"));
        BrokerEvent exchangeBinding = map(
                "binding.deleted",
                headers("source_name", "a", "destination_name", "b", "destination_kind", "exchange", "vhost", "/"));
        assertThat(exchangeBinding.type()).isEqualTo(EventTypes.RESOURCE_DELETED);
        assertThat(exchangeBinding.subject()).contains(id(RabbitMqKinds.EXCHANGE_BINDING, "/", "a", "b", "~"));
        assertThat(EventMapping.map(
                        "binding.created",
                        headers("source_name", "", "destination_name", "q", "vhost", "/"),
                        RECEIVED,
                        1,
                        QUORUM))
                .isEmpty();
    }

    @Test
    void aConsumerCreatedIsAnAttachedEventNamingItsSubscriptionUnderTheStandardKey() {
        BrokerEvent event = map(
                "consumer.created",
                headers("queue", "orders", "vhost", "/", "consumer_tag", "ctag-1", "channel", "<rabbit@a.1.2.3>"));
        assertThat(event.type()).isEqualTo(EventTypes.CONSUMER_ATTACHED);
        ResourceId queue = id(RabbitMqKinds.QUORUM_QUEUE, "/", "orders");
        assertThat(event.subject()).contains(queue);
        assertThat(event.data().get(StandardAttributes.SUBSCRIPTION)).contains(queue);
        assertThat(event.data().get(StandardAttributes.CONSUMER))
                .contains(id(RabbitMqKinds.CONSUMER, "/", "<rabbit@a.1.2.3>", "ctag-1"));
        assertThat(event.data().get(StandardAttributes.CHANNEL))
                .contains(id(RabbitMqKinds.CHANNEL, "<rabbit@a.1.2.3>"));
        assertThat(map("consumer.deleted", headers("queue", "orders", "vhost", "/"))
                        .type())
                .isEqualTo(EventTypes.CONSUMER_DETACHED);
    }

    @Test
    void connectionsAreClientsWithTheirPrincipalProtocolAndClientId() {
        BrokerEvent event = map(
                "connection.created",
                headers(
                        "name",
                        "10.0.0.1:5555 -> 10.0.0.2:5672",
                        "user",
                        "ann",
                        "protocol",
                        "AMQP 0-9-1",
                        "peer_host",
                        "10.0.0.1",
                        "client_properties",
                        Map.of("connection_name", "orders-app")));
        assertThat(event.type()).isEqualTo(EventTypes.CLIENT_CONNECTED);
        assertThat(event.data().get(StandardAttributes.PRINCIPAL)).contains("ann");
        assertThat(event.data().get(StandardAttributes.PROTOCOL)).contains("AMQP 0-9-1");
        assertThat(event.data().get(StandardAttributes.REMOTE_ADDRESS)).contains("10.0.0.1");
        assertThat(event.data().get(StandardAttributes.CLIENT_ID)).contains("orders-app");
        BrokerEvent closed = map("connection.closed", headers("name", "c1"));
        assertThat(closed.type()).isEqualTo(EventTypes.CLIENT_DISCONNECTED);
        assertThat(closed.data().get(StandardAttributes.CLIENT_ID)).contains("c1");
        assertThat(closed.subject()).contains(id(RabbitMqKinds.CONNECTION, "c1"));
    }

    @Test
    void channelsAreOpenedAndClosed() {
        BrokerEvent opened = map("channel.created", headers("name", "c1 (1)"));
        assertThat(opened.type()).isEqualTo(EventTypes.CHANNEL_OPENED);
        assertThat(opened.subject()).contains(id(RabbitMqKinds.CHANNEL, "c1 (1)"));
        assertThat(map("channel.closed", headers("name", "c1 (1)")).type()).isEqualTo(EventTypes.CHANNEL_CLOSED);
    }

    @Test
    void everythingElseIsNamespacedWithItsScalarHeadersAndNoSecrets() {
        BrokerEvent event = map(
                "alarm.set",
                headers(
                        "source",
                        "disk",
                        "node",
                        "rabbit@a",
                        "enabled",
                        true,
                        "limit",
                        5L,
                        "nested",
                        Map.of("a", 1),
                        "password_hash",
                        "abc",
                        "uri",
                        "amqp://x",
                        "user_who_performed_action",
                        "ann"));
        assertThat(event.type()).isEqualTo("rabbitmq.alarm.set");
        assertThat(event.subject()).isEmpty();
        assertThat(event.data().raw("rabbitmq.source")).contains("disk");
        assertThat(event.data().raw("rabbitmq.node")).contains("rabbit@a");
        assertThat(event.data().raw("rabbitmq.enabled")).contains("true");
        assertThat(event.data().raw("rabbitmq.limit")).contains("5");
        assertThat(event.data().raw("rabbitmq.nested")).isEmpty();
        assertThat(event.data().raw("rabbitmq.password_hash")).isEmpty();
        assertThat(event.data().raw("rabbitmq.uri")).isEmpty();
        assertThat(event.data().get(StandardAttributes.PRINCIPAL)).contains("ann");
        assertThat(event.data().get(AttributeKey.of("rabbitmq.user_who_performed_action", String.class)))
                .isEmpty();
        assertThat(map("node.down", headers()).type()).isEqualTo("rabbitmq.node.down");
    }

    @Test
    void theEventTakesItsTimeFromTheTimestampAndItsIdFromKeyTimeAndSequence() {
        BrokerEvent timed = map("vhost.created", headers("name", "eu", "timestamp_in_ms", 1_700_000_000_123L));
        assertThat(timed.time()).isEqualTo(Instant.ofEpochMilli(1_700_000_000_123L));
        assertThat(timed.id()).isEqualTo("vhost.created:1700000000123:7");
        BrokerEvent untimed = map("vhost.created", headers("name", "eu"));
        assertThat(untimed.time()).isEqualTo(RECEIVED);
    }

    @Test
    void aMessageThatLeavesOutANameStillMapsToAValidId() {
        BrokerEvent event = map("exchange.created", headers());
        assertThat(event.subject()).contains(id(RabbitMqKinds.EXCHANGE, "/", "amq.default"));
        assertThat(Optional.of(map("user.created", headers())).orElseThrow().subject())
                .contains(id(RabbitMqKinds.USER, "unknown"));
    }
}
