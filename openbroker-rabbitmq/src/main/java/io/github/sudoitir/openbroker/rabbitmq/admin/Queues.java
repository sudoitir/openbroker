package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.AttributeKey;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.MetricValue;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import tools.jackson.databind.JsonNode;

/** Reads classic queues, quorum queues and streams from the management API's JSON, and writes them back where the verbs allow. */
final class Queues {

    /** Queue states in which the queue serves nothing. */
    private static final Set<String> DOWN = Set.of("down", "stopped", "crashed", "minority");

    private final RabbitMqCluster cluster;

    Queues(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /**
     * The kind of the queue type {@code type}; a queue with no type is a classic queue, and a type the driver does
     * not list (such as an MQTT QoS 0 queue) has none.
     */
    static Optional<KindId> kind(String type) {
        return switch (type) {
            case "", "classic" -> Optional.of(RabbitMqKinds.CLASSIC_QUEUE);
            case "quorum" -> Optional.of(RabbitMqKinds.QUORUM_QUEUE);
            case "stream" -> Optional.of(RabbitMqKinds.STREAM);
            default -> Optional.empty();
        };
    }

    /** The resource that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        KindId kind = kind(json.path("type").asString())
                .orElseThrow(() -> RabbitMqErrors.badResponse(
                        "RabbitMQ listed a queue of type '" + json.path("type").asString()
                                + "', which this driver does not read.",
                        null));
        String vhost = json.path("vhost").asString();
        JsonNode arguments = json.path("arguments");
        JsonNode policy = json.path("effective_policy_definition");
        Attributes.Builder attributes = Attributes.builder();
        attributes.put(StandardAttributes.DURABLE, ReadSupport.flag(json, "durable"));
        attributes.putRaw(RabbitMqKinds.AUTO_DELETE, ReadSupport.flag(json, "auto_delete"));
        attributes.put(StandardAttributes.TEMPORARY, ReadSupport.flag(json, "exclusive"));
        metric(attributes, MetricKeys.BACKLOG, json, "messages_ready");
        metric(attributes, MetricKeys.BACKLOG_SIZE, json, "message_bytes");
        metric(attributes, MetricKeys.IN_FLIGHT, json, "messages_unacknowledged");
        if (json.path("consumers").isNumber()) {
            attributes.put(
                    MetricKeys.CONSUMER_COUNT,
                    MetricValue.exact(json.path("consumers").asLong()));
        }
        ReadSupport.text(json, "node").ifPresent(n -> attributes.put(StandardAttributes.PLACEMENT_NODE, n));
        setting(arguments, policy, "x-dead-letter-exchange", "dead-letter-exchange")
                .map(JsonNode::asString)
                .ifPresent(v -> attributes.put(StandardAttributes.DEAD_LETTER, v));
        ReadSupport.members(attributes, RabbitMqKinds.ARGUMENT_PREFIX, arguments);
        if (RabbitMqKinds.QUORUM_QUEUE.equals(kind)) {
            setting(arguments, policy, "x-delivery-limit", "delivery-limit")
                    .ifPresent(v -> attributes.putRaw(RabbitMqKinds.DELIVERY_LIMIT, v.asLong()));
            ReadSupport.put(attributes, RabbitMqKinds.LEADER, ReadSupport.text(json, "leader"));
        }
        if (RabbitMqKinds.STREAM.equals(kind)) {
            setting(arguments, policy, "x-max-age", "max-age")
                    .ifPresent(v -> attributes.putRaw(RabbitMqKinds.MAX_AGE, v.asString()));
            setting(arguments, policy, "x-max-length-bytes", "max-length-bytes")
                    .ifPresent(v -> attributes.putRaw(RabbitMqKinds.MAX_LENGTH_BYTES, v.asLong()));
            ReadSupport.putLong(attributes, RabbitMqKinds.COMMITTED_OFFSET, json, "committed_offset");
            ReadSupport.putLong(attributes, RabbitMqKinds.FIRST_TIMESTAMP, json, "first_timestamp");
        }
        ReadSupport.parent(attributes, vhost);
        List<Condition> conditions = new ArrayList<>();
        String state = json.path("state").asString();
        if (!state.isEmpty()) {
            boolean down = DOWN.contains(state);
            conditions.add(new Condition(
                    ConditionTypes.SERVING,
                    down ? ConditionStatus.FALSE : ConditionStatus.TRUE,
                    down ? state : "",
                    down ? "The queue is " + state + "." : "",
                    Optional.empty()));
        }
        Placements.underReplicated(kind, json).ifPresent(conditions::add);
        return new Resource(
                ResourceId.of(kind.value(), vhost, json.path("name").asString()),
                attributes.build(),
                conditions,
                Placements.of(kind, json),
                cluster.options().clock().instant());
    }

    /** A statistic the management API samples: present only when the listing carries it, always an estimate. */
    private static void metric(Attributes.Builder out, AttributeKey<MetricValue> key, JsonNode json, String field) {
        JsonNode value = json.path(field);
        if (value.isNumber()) {
            out.put(key, MetricValue.estimate(value.asDouble()));
        }
    }

    /** A queue setting: its declared argument, or else the effective policy's definition. */
    private static Optional<JsonNode> setting(JsonNode arguments, JsonNode policy, String argument, String definition) {
        if (arguments.has(argument)) {
            return Optional.of(arguments.path(argument));
        }
        return policy.has(definition) ? Optional.of(policy.path(definition)) : Optional.empty();
    }
}
