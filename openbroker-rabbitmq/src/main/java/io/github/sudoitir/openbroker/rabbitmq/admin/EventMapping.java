package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.AttributeKey;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Pure mapping of an event exchange message (routing key and headers) to a {@link BrokerEvent}. The event's id is
 * {@code <routing key>:<timestamp>:<sequence>}, its time the {@code timestamp_in_ms} header, and the
 * {@code user_who_performed_action} header its principal. A header that may hold a secret never becomes an attribute.
 */
final class EventMapping {

    /** The prefix of the event type of what the SDK has no standard type for. */
    static final String NAMESPACE = "rabbitmq.";

    private static final String VHOST = "vhost";
    private static final String NAME = "name";
    private static final String ARGUMENTS = "arguments";
    private static final String TIMESTAMP = "timestamp_in_ms";
    private static final String PERFORMED_BY = "user_who_performed_action";
    private static final String QUEUE_TYPE = "x-queue-type";
    private static final String CREATED = ".created";
    private static final String DELETED = ".deleted";
    private static final Set<String> SECRET_PARTS = Set.of("password", "secret", "token", "hash", "value", "uri");

    private EventMapping() {}

    /** Maps {@code routingKey} as of now, naming every queue a classic queue unless its arguments say otherwise. */
    static Optional<BrokerEvent> map(String routingKey, Map<String, Object> headers) {
        return map(routingKey, headers, Instant.now(), 0, (vhost, queue) -> RabbitMqKinds.CLASSIC_QUEUE);
    }

    /**
     * Maps one event.
     *
     * @param received when the message arrived, used when it carries no timestamp
     * @param sequence the count of messages read, which makes the id unique within a millisecond
     * @param queueKind the kind of a queue named by virtual host and name, for a queue whose arguments say none
     * @return the event, or empty for a message about the default exchange, which has no resource of its own
     */
    static Optional<BrokerEvent> map(
            String routingKey,
            Map<String, Object> headers,
            Instant received,
            long sequence,
            BiFunction<String, String, KindId> queueKind) {
        Instant time = timestamp(headers).orElse(received);
        String id = routingKey + ":" + time.toEpochMilli() + ":" + sequence;
        Attributes.Builder data = Attributes.builder();
        text(headers, PERFORMED_BY).ifPresent(user -> data.put(StandardAttributes.PRINCIPAL, user));
        Mapped mapped = mapped(routingKey, headers, queueKind, data);
        if (mapped == null) {
            return Optional.empty();
        }
        return Optional.of(
                new BrokerEvent(id, RabbitMqCluster.CLUSTER, mapped.type(), mapped.subject(), time, data.build()));
    }

    private record Mapped(String type, Optional<ResourceId> subject) {}

    private static Mapped mapped(
            String key, Map<String, Object> h, BiFunction<String, String, KindId> queueKind, Attributes.Builder data) {
        String vhost = text(h, VHOST).orElse("/");
        String name = text(h, NAME).orElse("");
        switch (key) {
            case "queue.created", "queue.deleted" -> {
                KindId kind = text(h, ARGUMENTS + "." + QUEUE_TYPE)
                        .flatMap(EventMapping::queueKind)
                        .orElseGet(() -> queueKind.apply(vhost, name));
                return change(key, resource(kind, vhost, name));
            }
            case "exchange.created", "exchange.deleted" -> {
                return change(key, resource(RabbitMqKinds.EXCHANGE, vhost, ManagementPaths.idName(name)));
            }
            case "vhost.created", "vhost.deleted" -> {
                return change(key, resource(RabbitMqKinds.VHOST, name));
            }
            case "policy.set", "policy.cleared" -> {
                String type = key.endsWith("set") ? EventTypes.RESOURCE_CREATED : EventTypes.RESOURCE_DELETED;
                return new Mapped(type, Optional.of(resource(RabbitMqKinds.POLICY, vhost, name)));
            }
            case "permission.created", "permission.deleted" -> {
                return change(
                        key,
                        resource(
                                RabbitMqKinds.PERMISSION, vhost, text(h, "user").orElse(name)));
            }
            case "user.created", "user.deleted" -> {
                return change(key, resource(RabbitMqKinds.USER, name));
            }
            case "binding.created", "binding.deleted" -> {
                return binding(key, h, vhost);
            }
            case "parameter.set", "parameter.cleared" -> {
                if ("shovel".equals(text(h, "component").orElse(""))) {
                    String type = key.endsWith("set") ? EventTypes.RESOURCE_CREATED : EventTypes.RESOURCE_DELETED;
                    return new Mapped(type, Optional.of(resource(RabbitMqKinds.SHOVEL, vhost, name)));
                }
            }
            case "consumer.created", "consumer.deleted" -> {
                return consumer(key, h, vhost, queueKind, data);
            }
            case "connection.created", "connection.closed" -> {
                return connection(key, h, name, data);
            }
            case "channel.created", "channel.closed" -> {
                String type = key.endsWith("created") ? EventTypes.CHANNEL_OPENED : EventTypes.CHANNEL_CLOSED;
                ResourceId channel = resource(RabbitMqKinds.CHANNEL, name);
                data.put(StandardAttributes.CHANNEL, channel);
                return new Mapped(type, Optional.of(channel));
            }
            default -> {
                // Everything else is carried under the driver's own namespace, below.
            }
        }
        return generic(key, h, data);
    }

    private static Mapped change(String key, ResourceId subject) {
        String type = key.endsWith(CREATED) ? EventTypes.RESOURCE_CREATED : EventTypes.RESOURCE_DELETED;
        return new Mapped(type, Optional.of(subject));
    }

    private static Mapped binding(String key, Map<String, Object> h, String vhost) {
        String source = ManagementPaths.idName(text(h, "source_name").orElse(""));
        if (ManagementPaths.DEFAULT_EXCHANGE.equals(source)) {
            return null;
        }
        String destination = ManagementPaths.idName(text(h, "destination_name").orElse(""));
        String properties = text(h, "routing_key").filter(k -> !k.isEmpty()).orElse("~");
        boolean toExchange = "exchange".equals(text(h, "destination_kind").orElse("queue"));
        KindId kind = toExchange ? RabbitMqKinds.EXCHANGE_BINDING : RabbitMqKinds.BINDING;
        return change(key, resource(kind, vhost, source, destination, properties));
    }

    private static Mapped consumer(
            String key,
            Map<String, Object> h,
            String vhost,
            BiFunction<String, String, KindId> queueKind,
            Attributes.Builder data) {
        String queue = text(h, "queue").orElse("");
        String channel = text(h, "channel").orElse("");
        String tag = text(h, "consumer_tag").orElse("");
        ResourceId subscription = resource(queueKind.apply(vhost, queue), vhost, queue);
        ResourceId consumer = resource(RabbitMqKinds.CONSUMER, vhost, channel, tag);
        data.put(StandardAttributes.SUBSCRIPTION, subscription);
        data.put(StandardAttributes.CONSUMER, consumer);
        data.put(StandardAttributes.CHANNEL, resource(RabbitMqKinds.CHANNEL, channel));
        String type = key.endsWith("created") ? EventTypes.CONSUMER_ATTACHED : EventTypes.CONSUMER_DETACHED;
        return new Mapped(type, Optional.of(subscription));
    }

    private static Mapped connection(String key, Map<String, Object> h, String name, Attributes.Builder data) {
        text(h, "user").ifPresent(user -> data.put(StandardAttributes.PRINCIPAL, user));
        text(h, "protocol").ifPresent(protocol -> data.put(StandardAttributes.PROTOCOL, protocol));
        text(h, "peer_host").ifPresent(host -> data.put(StandardAttributes.REMOTE_ADDRESS, host));
        text(h, "client_properties.connection_name")
                .or(() -> Optional.of(name).filter(n -> !n.isEmpty()))
                .ifPresent(client -> data.put(StandardAttributes.CLIENT_ID, client));
        String type = key.endsWith("created") ? EventTypes.CLIENT_CONNECTED : EventTypes.CLIENT_DISCONNECTED;
        return new Mapped(type, Optional.of(resource(RabbitMqKinds.CONNECTION, name)));
    }

    private static Mapped generic(String key, Map<String, Object> headers, Attributes.Builder data) {
        headers.forEach((header, value) -> {
            if (value == null || !scalar(value) || secret(header) || PERFORMED_BY.equals(header)) {
                return;
            }
            data.put(AttributeKey.of(NAMESPACE + header, String.class), value.toString());
        });
        return new Mapped(NAMESPACE + key, Optional.empty());
    }

    private static boolean scalar(Object value) {
        return value instanceof CharSequence || value instanceof Number || value instanceof Boolean;
    }

    private static boolean secret(String header) {
        String lower = header.toLowerCase(Locale.ROOT);
        return SECRET_PARTS.stream().anyMatch(lower::contains);
    }

    private static Optional<KindId> queueKind(String type) {
        return switch (type) {
            case "quorum" -> Optional.of(RabbitMqKinds.QUORUM_QUEUE);
            case "stream" -> Optional.of(RabbitMqKinds.STREAM);
            case "classic" -> Optional.of(RabbitMqKinds.CLASSIC_QUEUE);
            default -> Optional.empty();
        };
    }

    private static Optional<Instant> timestamp(Map<String, Object> headers) {
        Object value = headers.get(TIMESTAMP);
        if (value instanceof Number number) {
            return Optional.of(Instant.ofEpochMilli(number.longValue()));
        }
        return Optional.empty();
    }

    /** The id of a resource; a segment the message left out is named {@code unknown}, as no segment may be empty. */
    private static ResourceId resource(KindId kind, String... segments) {
        String[] named = new String[segments.length];
        for (int i = 0; i < segments.length; i++) {
            named[i] = segments[i].isEmpty() ? "unknown" : segments[i];
        }
        return new ResourceId(kind, ResourcePath.of(named));
    }

    /** A header as text; {@code arguments.x} reads the entry {@code x} of the {@code arguments} table. */
    private static Optional<String> text(Map<String, Object> headers, String name) {
        Object value = headers.get(name);
        if (value == null && name.startsWith(ARGUMENTS + ".") && headers.get(ARGUMENTS) instanceof Map<?, ?> table) {
            value = table.get(name.substring(ARGUMENTS.length() + 1));
        }
        if (value == null
                && name.startsWith("client_properties.")
                && headers.get("client_properties") instanceof Map<?, ?> table) {
            value = table.get(name.substring("client_properties.".length()));
        }
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(value.toString());
    }
}
