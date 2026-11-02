package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import tools.jackson.databind.JsonNode;

/** Reads connections, channels and consumers from the management API's JSON, and writes them back where the verbs allow. */
final class Clients {

    private static final String VHOST = "vhost";

    private final RabbitMqCluster cluster;

    Clients(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /** The kind of client that {@code json} is: a consumer names its tag, a channel its connection. */
    static KindId kind(JsonNode json) {
        if (json.has("consumer_tag")) {
            return RabbitMqKinds.CONSUMER;
        }
        return json.has("connection_details") ? RabbitMqKinds.CHANNEL : RabbitMqKinds.CONNECTION;
    }

    /** The connection, channel or consumer that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        KindId kind = kind(json);
        if (RabbitMqKinds.CONSUMER.equals(kind)) {
            return consumer(json);
        }
        return RabbitMqKinds.CHANNEL.equals(kind) ? channel(json) : connection(json);
    }

    private Resource connection(JsonNode json) {
        Attributes.Builder attributes = Attributes.builder();
        ReadSupport.put(attributes, StandardAttributes.PROTOCOL.name(), ReadSupport.text(json, "protocol"));
        ReadSupport.put(attributes, StandardAttributes.PRINCIPAL.name(), ReadSupport.text(json, "user"));
        ReadSupport.put(attributes, RabbitMqKinds.VHOST_NAME, ReadSupport.text(json, VHOST));
        ReadSupport.put(
                attributes,
                StandardAttributes.CLIENT_ID.name(),
                ReadSupport.text(json.path("client_properties"), "connection_name"));
        Optional<String> host = ReadSupport.text(json, "peer_host");
        if (host.isPresent()) {
            String port = ReadSupport.text(json, "peer_port").map(p -> ":" + p).orElse("");
            attributes.put(StandardAttributes.REMOTE_ADDRESS, host.get() + port);
        }
        JsonNode connected = json.path("connected_at");
        if (connected.isNumber()) {
            attributes.put(StandardAttributes.CREATED_AT, Instant.ofEpochMilli(connected.asLong()));
        }
        ReadSupport.putLong(attributes, RabbitMqKinds.CHANNELS, json, "channels");
        Optional<String> node = ReadSupport.text(json, "node");
        node.ifPresent(n -> attributes.put(StandardAttributes.PLACEMENT_NODE, n));
        return new Resource(
                ResourceId.of(
                        RabbitMqKinds.CONNECTION.value(), json.path("name").asString()),
                attributes.build(),
                List.of(),
                placement(node),
                cluster.options().clock().instant());
    }

    private Resource channel(JsonNode json) {
        JsonNode connection = json.path("connection_details");
        Attributes.Builder attributes = Attributes.builder();
        ReadSupport.text(connection, "name")
                .ifPresent(c -> attributes.put(
                        StandardAttributes.CONNECTION, ResourceId.of(RabbitMqKinds.CONNECTION.value(), c)));
        ReadSupport.putLong(attributes, RabbitMqKinds.PREFETCH_COUNT, json, "prefetch_count");
        ReadSupport.putLong(attributes, RabbitMqKinds.UNACKED, json, "messages_unacknowledged");
        attributes.putRaw(RabbitMqKinds.CONFIRM, ReadSupport.flag(json, "confirm"));
        attributes.putRaw(RabbitMqKinds.TRANSACTIONAL, ReadSupport.flag(json, "transactional"));
        JsonNode consumers = json.path("consumer_count");
        if (consumers.isNumber()) {
            attributes.put(StandardAttributes.CONSUMER_COUNT, consumers.asLong());
        }
        ReadSupport.put(attributes, RabbitMqKinds.VHOST_NAME, ReadSupport.text(json, VHOST));
        ReadSupport.put(attributes, StandardAttributes.PRINCIPAL.name(), ReadSupport.text(json, "user"));
        Optional<String> node = ReadSupport.text(json, "node");
        node.ifPresent(n -> attributes.put(StandardAttributes.PLACEMENT_NODE, n));
        return new Resource(
                ResourceId.of(RabbitMqKinds.CHANNEL.value(), json.path("name").asString()),
                attributes.build(),
                List.of(),
                placement(node),
                cluster.options().clock().instant());
    }

    private Resource consumer(JsonNode json) {
        String vhost = json.path("queue").path(VHOST).asString();
        String queue = json.path("queue").path("name").asString();
        JsonNode channelDetails = json.path("channel_details");
        String channel = channelDetails.path("name").asString();
        Attributes.Builder attributes = Attributes.builder();
        attributes.put(
                StandardAttributes.SUBSCRIPTION,
                ResourceId.of(cluster.queueTypes().kindOf(vhost, queue).value(), vhost, queue));
        attributes.put(StandardAttributes.CHANNEL, ResourceId.of(RabbitMqKinds.CHANNEL.value(), channel));
        ReadSupport.text(channelDetails, "connection_name")
                .ifPresent(c -> attributes.put(
                        StandardAttributes.CONNECTION, ResourceId.of(RabbitMqKinds.CONNECTION.value(), c)));
        attributes.putRaw(RabbitMqKinds.ACK_REQUIRED, ReadSupport.flag(json, "ack_required"));
        ReadSupport.putLong(attributes, RabbitMqKinds.PREFETCH_COUNT, json, "prefetch_count");
        attributes.putRaw(RabbitMqKinds.ACTIVE, ReadSupport.flag(json, "active"));
        attributes.putRaw(RabbitMqKinds.EXCLUSIVE, ReadSupport.flag(json, "exclusive"));
        ReadSupport.parent(attributes, vhost);
        Optional<String> node = ReadSupport.text(channelDetails, "node");
        return new Resource(
                ResourceId.of(
                        RabbitMqKinds.CONSUMER.value(),
                        vhost,
                        channel,
                        json.path("consumer_tag").asString()),
                attributes.build(),
                List.of(),
                placement(node),
                cluster.options().clock().instant());
    }

    private static List<Placement> placement(Optional<String> node) {
        return node.map(n -> List.of(Placement.instance(NodeId.of(n), Attributes.empty())))
                .orElseGet(ReadSupport::onCluster);
    }
}
