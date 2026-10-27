package io.github.sudoitir.openbroker.kafka.admin;

import static io.github.sudoitir.openbroker.model.Verb.BROWSE;
import static io.github.sudoitir.openbroker.model.Verb.CREATE;
import static io.github.sudoitir.openbroker.model.Verb.DELETE;
import static io.github.sudoitir.openbroker.model.Verb.GET;
import static io.github.sudoitir.openbroker.model.Verb.LIST;
import static io.github.sudoitir.openbroker.model.Verb.PURGE;
import static io.github.sudoitir.openbroker.model.Verb.RESOLVE;
import static io.github.sudoitir.openbroker.model.Verb.SEEK;
import static io.github.sudoitir.openbroker.model.Verb.SEND;
import static io.github.sudoitir.openbroker.model.Verb.TRUNCATE;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** The kinds the Kafka driver defines, their native attributes and the shape of their ids. */
public final class KafkaKinds {

    /** A topic: a destination. Its id is {@code [topic]}. */
    public static final KindId TOPIC = KindId.of("kafka.topic");
    /** A partition of a topic. Its id is {@code [topic, partition]}. */
    public static final KindId PARTITION = KindId.of("kafka.partition");
    /** A group of any type, consumer, classic, share or streams. Its id is {@code [group]}. */
    public static final KindId GROUP = KindId.of("kafka.group");
    /** A consumer group on a topic. Its id is {@code [group, topic]}. */
    public static final KindId GROUP_TOPIC = KindId.of("kafka.group-topic");
    /** A share group on a topic. Its id is {@code [group, topic]}. */
    public static final KindId SHARE_GROUP_TOPIC = KindId.of("kafka.share-group-topic");
    /**
     * An ACL: an access rule. Its id is {@code [principal, host, operation, permission, resource type,
     * pattern type, resource name]}.
     */
    public static final KindId ACL = KindId.of("kafka.acl");
    /** A transactional id. Its id is {@code [transactional id]}. */
    public static final KindId TRANSACTIONAL_ID = KindId.of("kafka.transactional-id");

    // Native attributes, namespaced.

    public static final String PARTITIONS = "kafka.partitions";
    public static final String REPLICATION_FACTOR = "kafka.replication_factor";
    public static final String TOPIC_ID = "kafka.topic_id";
    public static final String INTERNAL = "kafka.internal";
    /** A topic setting, such as {@code kafka.config.retention.ms}. An empty value on update removes the override. */
    public static final String CONFIG_PREFIX = "kafka.config.";

    public static final String LEADER = "kafka.leader";
    public static final String REPLICAS = "kafka.replicas";
    public static final String IN_SYNC_REPLICAS = "kafka.isr";
    public static final String LOG_START_OFFSET = "kafka.log_start_offset";
    public static final String LOG_END_OFFSET = "kafka.log_end_offset";
    public static final String SIZE_BYTES = "kafka.size_bytes";

    public static final String GROUP_TYPE = "kafka.group_type";
    public static final String GROUP_STATE = "kafka.group_state";
    public static final String GROUP_EPOCH = "kafka.group_epoch";
    public static final String MEMBERS = "kafka.members";
    public static final String COORDINATOR = "kafka.coordinator";
    public static final String ASSIGNOR = "kafka.assignor";
    /** Why a group's offsets cannot be changed through this driver; absent when they can. */
    public static final String READ_ONLY_REASON = "kafka.read_only_reason";

    /** A subscription's committed offsets (a consumer group) or start offsets (a share group), by partition. */
    public static final String OFFSETS = "kafka.offsets";
    /** The partitions whose lag is not known, with why. */
    public static final String LAG_UNAVAILABLE = "kafka.lag_unavailable";

    public static final String PRINCIPAL = "kafka.principal";
    public static final String HOST = "kafka.host";
    public static final String OPERATION = "kafka.operation";
    public static final String PERMISSION = "kafka.permission";
    public static final String RESOURCE_TYPE = "kafka.resource_type";
    public static final String PATTERN_TYPE = "kafka.pattern_type";
    public static final String RESOURCE_NAME = "kafka.resource_name";

    public static final String PRODUCER_ID = "kafka.producer_id";
    public static final String PRODUCER_EPOCH = "kafka.producer_epoch";
    public static final String TRANSACTION_STATE = "kafka.transaction_state";
    public static final String TRANSACTION_TIMEOUT_MS = "kafka.transaction_timeout_ms";
    public static final String TRANSACTION_START = "kafka.transaction_start";
    public static final String TRANSACTION_PARTITIONS = "kafka.transaction_partitions";

    /** The condition that says a backlog is an estimate, and why. */
    public static final String BACKLOG_ESTIMATE = "kafka.backlog_estimate";

    private static final String MESSAGE = "{message}";

    private KafkaKinds() {}

    /** The kinds every Kafka cluster has. The share-group kind is added where the cluster runs share groups. */
    static List<Kind> base() {
        List<Kind> kinds = new ArrayList<>();
        kinds.add(kind(
                TOPIC,
                KindLabel.of("Topic", "Topics"),
                ResourceClass.DESTINATION,
                Set.of(LIST, GET, CREATE, UPDATE, DELETE, TRUNCATE, BROWSE, SEND),
                Optional.empty(),
                List.of(
                        AttributeSpec.config(PARTITIONS, true, false),
                        AttributeSpec.config(REPLICATION_FACTOR, false, true),
                        AttributeSpec.config(CONFIG_PREFIX + "cleanup.policy", true, false),
                        AttributeSpec.config(CONFIG_PREFIX + "retention.ms", true, false),
                        AttributeSpec.config(CONFIG_PREFIX + "retention.bytes", true, false),
                        AttributeSpec.config(CONFIG_PREFIX + "max.message.bytes", true, false),
                        AttributeSpec.config(CONFIG_PREFIX + "min.insync.replicas", true, false),
                        AttributeSpec.status(TOPIC_ID),
                        AttributeSpec.status(INTERNAL))));
        kinds.add(kind(
                PARTITION,
                KindLabel.of("Partition", "Partitions"),
                ResourceClass.PARTITION,
                Set.of(LIST, GET, TRUNCATE, BROWSE),
                Optional.of(TOPIC),
                List.of(
                        AttributeSpec.status(LEADER),
                        AttributeSpec.status(REPLICAS),
                        AttributeSpec.status(IN_SYNC_REPLICAS),
                        AttributeSpec.metric(LOG_START_OFFSET, Instrument.GAUGE, "{offset}"),
                        AttributeSpec.metric(LOG_END_OFFSET, Instrument.GAUGE, "{offset}"),
                        AttributeSpec.metric(SIZE_BYTES, Instrument.GAUGE, "By"))));
        kinds.add(kind(
                GROUP,
                KindLabel.of("Group", "Groups"),
                ResourceClass.CONSUMER_GROUP,
                Set.of(LIST, GET, DELETE),
                Optional.empty(),
                List.of(
                        AttributeSpec.status(GROUP_TYPE),
                        AttributeSpec.status(GROUP_STATE),
                        AttributeSpec.status(GROUP_EPOCH),
                        AttributeSpec.status(MEMBERS),
                        AttributeSpec.status(COORDINATOR),
                        AttributeSpec.status(ASSIGNOR),
                        AttributeSpec.status(READ_ONLY_REASON))));
        kinds.add(kind(
                GROUP_TOPIC,
                KindLabel.of("Group on topic", "Groups on topics"),
                ResourceClass.SUBSCRIPTION,
                Set.of(LIST, GET, CREATE, DELETE, PURGE, SEEK, BROWSE, SEND),
                Optional.of(GROUP),
                subscriptionAttributes()));
        kinds.add(kind(
                ACL,
                KindLabel.of("ACL", "ACLs"),
                ResourceClass.ACCESS_RULE,
                Set.of(LIST, GET, CREATE, DELETE),
                Optional.empty(),
                List.of(
                        AttributeSpec.config(PRINCIPAL, false, true),
                        AttributeSpec.config(HOST, false, true),
                        AttributeSpec.config(OPERATION, false, true),
                        AttributeSpec.config(PERMISSION, false, true),
                        AttributeSpec.config(RESOURCE_TYPE, false, true),
                        AttributeSpec.config(PATTERN_TYPE, false, true),
                        AttributeSpec.config(RESOURCE_NAME, false, true))));
        kinds.add(kind(
                TRANSACTIONAL_ID,
                KindLabel.of("Transactional ID", "Transactional IDs"),
                ResourceClass.TRANSACTION,
                Set.of(LIST, GET, RESOLVE),
                Optional.empty(),
                List.of(
                        AttributeSpec.status(PRODUCER_ID),
                        AttributeSpec.status(PRODUCER_EPOCH),
                        AttributeSpec.status(TRANSACTION_STATE),
                        AttributeSpec.status(TRANSACTION_TIMEOUT_MS),
                        AttributeSpec.status(TRANSACTION_START),
                        AttributeSpec.status(TRANSACTION_PARTITIONS))));
        return kinds;
    }

    /** The share-group subscription kind, declared only where the cluster runs share groups in production. */
    static Kind shareGroupTopic() {
        return kind(
                SHARE_GROUP_TOPIC,
                KindLabel.of("Share group on topic", "Share groups on topics"),
                ResourceClass.SUBSCRIPTION,
                Set.of(LIST, GET, DELETE, PURGE, SEEK, BROWSE, SEND),
                Optional.of(GROUP),
                subscriptionAttributes());
    }

    private static List<AttributeSpec> subscriptionAttributes() {
        return List.of(
                AttributeSpec.metric(MetricKeys.BACKLOG.name(), Instrument.UP_DOWN_COUNTER, MESSAGE),
                AttributeSpec.metric(MetricKeys.CONSUMER_COUNT.name(), Instrument.UP_DOWN_COUNTER, "{consumer}"),
                AttributeSpec.status(OFFSETS),
                AttributeSpec.status(LAG_UNAVAILABLE),
                AttributeSpec.status(GROUP_STATE),
                AttributeSpec.status(GROUP_EPOCH));
    }

    private static Kind kind(
            KindId id,
            KindLabel label,
            ResourceClass resourceClass,
            Set<Verb> verbs,
            Optional<KindId> parent,
            List<AttributeSpec> attributes) {
        return new Kind(id, label, resourceClass, verbs, parent, new AttributeSchema(attributes), Set.of("name"));
    }
}
