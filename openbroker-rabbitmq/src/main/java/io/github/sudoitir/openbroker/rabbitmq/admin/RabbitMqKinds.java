package io.github.sudoitir.openbroker.rabbitmq.admin;

import static io.github.sudoitir.openbroker.model.Verb.BROWSE;
import static io.github.sudoitir.openbroker.model.Verb.CLOSE;
import static io.github.sudoitir.openbroker.model.Verb.CREATE;
import static io.github.sudoitir.openbroker.model.Verb.DELETE;
import static io.github.sudoitir.openbroker.model.Verb.GET;
import static io.github.sudoitir.openbroker.model.Verb.LIST;
import static io.github.sudoitir.openbroker.model.Verb.PURGE;
import static io.github.sudoitir.openbroker.model.Verb.RECEIVE;
import static io.github.sudoitir.openbroker.model.Verb.SEND;
import static io.github.sudoitir.openbroker.model.Verb.UPDATE;

import io.github.sudoitir.openbroker.model.AttributeSchema;
import io.github.sudoitir.openbroker.model.AttributeSpec;
import io.github.sudoitir.openbroker.model.Instrument;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.KindLabel;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.Verb;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Every {@link KindId} of the driver, its {@code rabbitmq.*} attribute keys and its {@link Kind} declarations. */
public final class RabbitMqKinds {

    public static final KindId VHOST = KindId.of("rabbitmq.vhost");
    public static final KindId EXCHANGE = KindId.of("rabbitmq.exchange");
    public static final KindId CLASSIC_QUEUE = KindId.of("rabbitmq.classic-queue");
    public static final KindId QUORUM_QUEUE = KindId.of("rabbitmq.quorum-queue");
    public static final KindId STREAM = KindId.of("rabbitmq.stream");
    public static final KindId BINDING = KindId.of("rabbitmq.binding");
    public static final KindId EXCHANGE_BINDING = KindId.of("rabbitmq.exchange-binding");
    public static final KindId SHOVEL = KindId.of("rabbitmq.shovel");
    public static final KindId FEDERATION_LINK = KindId.of("rabbitmq.federation-link");
    public static final KindId POLICY = KindId.of("rabbitmq.policy");
    public static final KindId OPERATOR_POLICY = KindId.of("rabbitmq.operator-policy");
    public static final KindId PERMISSION = KindId.of("rabbitmq.permission");
    public static final KindId TOPIC_PERMISSION = KindId.of("rabbitmq.topic-permission");
    public static final KindId USER = KindId.of("rabbitmq.user");
    public static final KindId CONNECTION = KindId.of("rabbitmq.connection");
    public static final KindId CHANNEL = KindId.of("rabbitmq.channel");
    public static final KindId CONSUMER = KindId.of("rabbitmq.consumer");

    /** An argument of an exchange, queue or binding, such as {@code rabbitmq.argument.x-max-length}. */
    public static final String ARGUMENT_PREFIX = "rabbitmq.argument.";
    /** A policy definition key, such as {@code rabbitmq.definition.max-length}; the value is JSON as text. */
    public static final String DEFINITION_PREFIX = "rabbitmq.definition.";

    public static final String DESCRIPTION = "rabbitmq.description";
    public static final String TAGS = "rabbitmq.tags";
    public static final String DEFAULT_QUEUE_TYPE = "rabbitmq.default_queue_type";
    public static final String EXCHANGE_TYPE = "rabbitmq.exchange_type";
    public static final String AUTO_DELETE = "rabbitmq.auto_delete";
    public static final String INTERNAL = "rabbitmq.internal";
    public static final String DELIVERY_LIMIT = "rabbitmq.delivery_limit";
    public static final String LEADER = "rabbitmq.leader";
    public static final String MAX_AGE = "rabbitmq.max_age";
    public static final String MAX_LENGTH_BYTES = "rabbitmq.max_length_bytes";
    public static final String COMMITTED_OFFSET = "rabbitmq.committed_offset";
    public static final String FIRST_TIMESTAMP = "rabbitmq.first_timestamp";
    public static final String PATTERN = "rabbitmq.pattern";
    public static final String APPLY_TO = "rabbitmq.apply_to";
    public static final String PRIORITY = "rabbitmq.priority";
    public static final String CONFIGURE = "rabbitmq.configure";
    public static final String WRITE = "rabbitmq.write";
    public static final String READ = "rabbitmq.read";
    public static final String VHOST_NAME = "rabbitmq.vhost";
    public static final String CHANNELS = "rabbitmq.channels";
    public static final String PREFETCH_COUNT = "rabbitmq.prefetch_count";
    public static final String UNACKED = "rabbitmq.unacked";
    public static final String CONFIRM = "rabbitmq.confirm";
    public static final String TRANSACTIONAL = "rabbitmq.transactional";
    public static final String ACK_REQUIRED = "rabbitmq.ack_required";
    public static final String ACTIVE = "rabbitmq.active";
    public static final String EXCLUSIVE = "rabbitmq.exclusive";

    private static final String MESSAGE = "{message}";
    private static final String BYTES = "By";

    private RabbitMqKinds() {}

    /** The declarations of all kinds, in the order above. */
    public static List<Kind> all() {
        Set<Verb> queue = Set.of(LIST, GET, CREATE, DELETE, PURGE, SEND, RECEIVE);
        Set<Verb> policy = Set.of(LIST, GET, CREATE, UPDATE, DELETE);
        Set<Verb> wiring = Set.of(LIST, GET, CREATE, DELETE);
        return List.of(
                kind(
                        VHOST,
                        "Virtual host",
                        "Virtual hosts",
                        ResourceClass.NAMESPACE,
                        Set.of(LIST, GET, CREATE, UPDATE, DELETE),
                        null,
                        config(DESCRIPTION, true),
                        config(TAGS, true),
                        config(DEFAULT_QUEUE_TYPE, true)),
                kind(
                        EXCHANGE,
                        "Exchange",
                        "Exchanges",
                        ResourceClass.DESTINATION,
                        Set.of(LIST, GET, CREATE, DELETE, SEND),
                        VHOST,
                        AttributeSpec.config(EXCHANGE_TYPE, false, true),
                        AttributeSpec.config("openbroker.durable", false, true),
                        AttributeSpec.config(AUTO_DELETE, false, true),
                        AttributeSpec.config(INTERNAL, false, true)),
                kind(
                        CLASSIC_QUEUE,
                        "Classic queue",
                        "Classic queues",
                        ResourceClass.SUBSCRIPTION,
                        queue,
                        VHOST,
                        queueAttributes()),
                kind(
                        QUORUM_QUEUE,
                        "Quorum queue",
                        "Quorum queues",
                        ResourceClass.SUBSCRIPTION,
                        queue,
                        VHOST,
                        with(
                                queueAttributes(),
                                AttributeSpec.config(DELIVERY_LIMIT, true, false),
                                AttributeSpec.status(LEADER))),
                kind(
                        STREAM,
                        "Stream",
                        "Streams",
                        ResourceClass.SUBSCRIPTION,
                        Set.of(LIST, GET, CREATE, DELETE, BROWSE, SEND),
                        VHOST,
                        with(
                                queueAttributes(),
                                AttributeSpec.config(MAX_AGE, false, true),
                                AttributeSpec.config(MAX_LENGTH_BYTES, false, true),
                                AttributeSpec.status(COMMITTED_OFFSET),
                                AttributeSpec.status(FIRST_TIMESTAMP))),
                kind(
                        BINDING,
                        "Binding",
                        "Bindings",
                        ResourceClass.ROUTE,
                        wiring,
                        VHOST,
                        AttributeSpec.status("openbroker.filter")),
                kind(
                        EXCHANGE_BINDING,
                        "Exchange binding",
                        "Exchange bindings",
                        ResourceClass.ROUTE,
                        wiring,
                        VHOST,
                        AttributeSpec.status("openbroker.filter")),
                kind(SHOVEL, "Shovel", "Shovels", ResourceClass.ROUTE, Set.of(LIST, GET, DELETE), VHOST),
                kind(
                        FEDERATION_LINK,
                        "Federation link",
                        "Federation links",
                        ResourceClass.ROUTE,
                        Set.of(LIST, GET),
                        VHOST),
                kind(
                        POLICY,
                        "Policy",
                        "Policies",
                        ResourceClass.POLICY,
                        policy,
                        VHOST,
                        config(PATTERN, true),
                        config(APPLY_TO, true),
                        config(PRIORITY, true)),
                kind(
                        OPERATOR_POLICY,
                        "Operator policy",
                        "Operator policies",
                        ResourceClass.POLICY,
                        policy,
                        VHOST,
                        config(PATTERN, true),
                        config(APPLY_TO, true),
                        config(PRIORITY, true)),
                kind(
                        PERMISSION,
                        "Permission",
                        "Permissions",
                        ResourceClass.ACCESS_RULE,
                        policy,
                        VHOST,
                        config(CONFIGURE, true),
                        config(WRITE, true),
                        config(READ, true)),
                kind(
                        TOPIC_PERMISSION,
                        "Topic permission",
                        "Topic permissions",
                        ResourceClass.ACCESS_RULE,
                        policy,
                        VHOST,
                        config(WRITE, true),
                        config(READ, true)),
                kind(
                        USER,
                        "User",
                        "Users",
                        ResourceClass.PRINCIPAL,
                        Set.of(LIST, GET),
                        null,
                        AttributeSpec.status(TAGS)),
                kind(
                        CONNECTION,
                        "Connection",
                        "Connections",
                        ResourceClass.CONNECTION,
                        Set.of(LIST, GET, CLOSE),
                        null,
                        AttributeSpec.status(VHOST_NAME),
                        AttributeSpec.status(CHANNELS)),
                kind(
                        CHANNEL,
                        "Channel",
                        "Channels",
                        ResourceClass.CHANNEL,
                        Set.of(LIST, GET),
                        null,
                        AttributeSpec.status(PREFETCH_COUNT),
                        AttributeSpec.status(UNACKED),
                        AttributeSpec.status(CONFIRM),
                        AttributeSpec.status(TRANSACTIONAL)),
                kind(
                        CONSUMER,
                        "Consumer",
                        "Consumers",
                        ResourceClass.CONSUMER,
                        Set.of(LIST),
                        VHOST,
                        AttributeSpec.status(ACK_REQUIRED),
                        AttributeSpec.status(PREFETCH_COUNT),
                        AttributeSpec.status(ACTIVE),
                        AttributeSpec.status(EXCLUSIVE)));
    }

    private static AttributeSpec config(String key, boolean writable) {
        return AttributeSpec.config(key, writable, false);
    }

    private static AttributeSpec[] queueAttributes() {
        return new AttributeSpec[] {
            AttributeSpec.config("openbroker.durable", false, true),
            AttributeSpec.config(AUTO_DELETE, false, true),
            AttributeSpec.metric(MetricKeys.BACKLOG.name(), Instrument.GAUGE, MESSAGE),
            AttributeSpec.metric(MetricKeys.BACKLOG_SIZE.name(), Instrument.GAUGE, BYTES),
            AttributeSpec.metric(MetricKeys.IN_FLIGHT.name(), Instrument.GAUGE, MESSAGE),
            AttributeSpec.metric(MetricKeys.CONSUMER_COUNT.name(), Instrument.GAUGE, "{consumer}")
        };
    }

    private static AttributeSpec[] with(AttributeSpec[] base, AttributeSpec... more) {
        AttributeSpec[] out = new AttributeSpec[base.length + more.length];
        System.arraycopy(base, 0, out, 0, base.length);
        System.arraycopy(more, 0, out, base.length, more.length);
        return out;
    }

    private static Kind kind(
            KindId id,
            String singular,
            String plural,
            ResourceClass resourceClass,
            Set<Verb> verbs,
            KindId parent,
            AttributeSpec... attributes) {
        return new Kind(
                id,
                KindLabel.of(singular, plural),
                resourceClass,
                verbs,
                Optional.ofNullable(parent),
                new AttributeSchema(List.of(attributes)),
                Set.of("name"));
    }
}
