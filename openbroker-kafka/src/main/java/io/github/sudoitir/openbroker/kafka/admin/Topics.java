package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.Delivery;
import io.github.sudoitir.openbroker.model.DeliveryModes;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.operation.AttributeChange;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.EtagBasis;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;

/** Topics: destinations with their partitions and settings. */
final class Topics implements Handler {

    private static final DeliveryModes DELIVERY = DeliveryModes.of(Delivery.POINT_TO_POINT, Delivery.PUBLISH_SUBSCRIBE);

    private final Kind kind;

    Topics(Kind kind) {
        this.kind = kind;
    }

    @Override
    public Kind kind() {
        return kind;
    }

    @Override
    public Set<String> selectors() {
        return Set.of();
    }

    static ResourceId id(String topic) {
        return new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(topic));
    }

    @Override
    public List<Candidate> candidates(Context context, Listing listing) {
        Set<String> names = new TreeSet<>(context.await()
                .get(context.admin()
                        .listTopics(new ListTopicsOptions()
                                .listInternal(true)
                                .timeoutMs(context.await().timeoutMs()))
                        .names()));
        var match = listing.names();
        List<Candidate> out = new ArrayList<>();
        for (String name : names) {
            if (match.test(name)) {
                out.add(new Candidate(
                        id(name),
                        List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                        Attributes.empty()));
            }
        }
        return out;
    }

    @Override
    public List<Resource> resources(Context context, List<Candidate> page, Listing listing) {
        if (page.isEmpty()) {
            return List.of();
        }
        List<String> names = page.stream().map(c -> c.id().name()).toList();
        Map<String, TopicDescription> described = describeListed(context, names);
        Map<String, Config> configs =
                listing.wantsPrefix(KafkaKinds.CONFIG_PREFIX) ? configs(context, described.keySet()) : Map.of();
        List<Resource> out = new ArrayList<>();
        for (Candidate candidate : page) {
            TopicDescription topic = described.get(candidate.id().name());
            if (topic != null) {
                out.add(resource(context, candidate, topic, configs.get(topic.name()), listing));
            }
        }
        return out;
    }

    @Override
    public Optional<Resource> find(Context context, ResourceId id) {
        Listing everything = new Listing(ResourceQuery.of(KafkaKinds.TOPIC));
        Map<String, TopicDescription> described;
        try {
            described = describe(context, List.of(id.name()));
        } catch (BrokerException e) {
            if (e.status() == Status.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
        TopicDescription topic = described.get(id.name());
        if (topic == null) {
            return Optional.empty();
        }
        Map<String, Config> configs = configs(context, Set.of(topic.name()));
        return Optional.of(resource(
                context,
                new Candidate(
                        id, List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())), Attributes.empty()),
                topic,
                configs.get(topic.name()),
                everything));
    }

    private static Map<String, TopicDescription> describe(Context context, Collection<String> names) {
        return describe(context, names, names.size() == 1);
    }

    private static Map<String, TopicDescription> describeSome(Context context, Collection<String> names) {
        return describe(context, names, false);
    }

    private static Map<String, TopicDescription> describe(Context context, Collection<String> names, boolean strict) {
        Map<String, KafkaFuture<TopicDescription>> pending = context.admin()
                .describeTopics(
                        TopicCollection.ofTopicNames(names),
                        new DescribeTopicsOptions().timeoutMs(context.await().timeoutMs()))
                .topicNameValues();
        Map<String, TopicDescription> out = new HashMap<>();
        for (String name : names) {
            try {
                out.put(name, context.await().get(pending.get(name)));
            } catch (BrokerException e) {
                // A topic deleted since it was listed is no longer there.
                if (e.status() != Status.NOT_FOUND || strict) {
                    throw e;
                }
            }
        }
        return out;
    }

    /**
     * Describes topics a listing just named. A broker can list a topic another has not heard of
     * yet, and answers that it hosts no such topic for a moment; one the listing named is waited for
     * a few reads before it is left out.
     */
    private static Map<String, TopicDescription> describeListed(Context context, List<String> names) {
        Map<String, TopicDescription> found = new HashMap<>();
        List<String> missing = new ArrayList<>(names);
        for (int attempt = 0; attempt < 5 && !missing.isEmpty(); attempt++) {
            if (attempt > 0) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException interrupted) {
                    throw Deadlines.cancelled(KafkaCluster.CLUSTER, interrupted);
                }
            }
            found.putAll(describeSome(context, missing));
            missing.removeAll(found.keySet());
        }
        return found;
    }

    private static Map<String, Config> configs(Context context, Collection<String> names) {
        List<ConfigResource> resources = names.stream()
                .map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n))
                .toList();
        Map<ConfigResource, Config> all = context.await()
                .get(context.admin()
                        .describeConfigs(
                                resources,
                                new DescribeConfigsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
        Map<String, Config> out = new HashMap<>();
        all.forEach((r, c) -> out.put(r.name(), c));
        return out;
    }

    private Resource resource(
            Context context, Candidate candidate, TopicDescription topic, Config config, Listing listing) {
        Attributes.Builder attributes = Attributes.builder()
                .put(StandardAttributes.DELIVERY, DELIVERY)
                .putRaw(KafkaKinds.PARTITIONS, (long) topic.partitions().size())
                .putRaw(
                        KafkaKinds.REPLICATION_FACTOR,
                        topic.partitions().isEmpty()
                                ? 0L
                                : (long)
                                        topic.partitions().getFirst().replicas().size())
                .putRaw(KafkaKinds.TOPIC_ID, String.valueOf(topic.topicId()))
                .putRaw(KafkaKinds.INTERNAL, topic.isInternal());
        if (config != null) {
            for (ConfigEntry entry : config.entries()) {
                if (!entry.isSensitive() && entry.value() != null) {
                    attributes.putRaw(KafkaKinds.CONFIG_PREFIX + entry.name(), entry.value());
                }
            }
        }
        long outOfSync = topic.partitions().stream()
                .filter(p -> p.isr().size() < p.replicas().size())
                .count();
        List<Condition> conditions = List.of(new Condition(
                ConditionTypes.UNDER_REPLICATED,
                outOfSync > 0 ? ConditionStatus.TRUE : ConditionStatus.FALSE,
                outOfSync > 0 ? "OUT_OF_SYNC_REPLICAS" : "",
                outOfSync > 0 ? outOfSync + " partitions have a replica that is not in sync." : "",
                Optional.empty()));
        return new Resource(
                candidate.id(),
                Attrs.project(attributes.build(), listing.query().projection()),
                conditions,
                candidate.placements(),
                context.now());
    }

    /** Why an internal topic is refused. The same words the configuration screen uses. */
    static final String INTERNAL_REASON = "The cluster manages internal topics.";

    private static final String INTERNAL_PREFIX = "__";

    private static final String CREATE_NOTE = "The cluster accepted this topic. Nothing has been created.";

    private static final String DELETE_NOTE =
            "Deleting this topic does not erase those groups' offsets. The groups will show the topic as deleted. "
                    + Subscriptions.ESTIMATE_REASON;

    private static final String INCREASE_NOTE =
            "Records already written stay on their partitions. A key can land on a new partition afterwards.";

    // Operations.

    @Override
    public Preview preview(Context context, ResourceId id, Operation operation, Scope scope) {
        switch (operation) {
            case Operation.Create create -> {
                checkCreate(context, id, create.attributes());
                validateNewTopic(context, id, create.attributes());
                List<AttributeChange> diff = new ArrayList<>(Attrs.diff(Attributes.empty(), create.attributes()));
                diff.add(new AttributeChange(KafkaKinds.NOTE, Optional.empty(), Optional.of(CREATE_NOTE)));
                return new Preview(
                        diff,
                        Set.of(),
                        new Estimate(Count.exact(1), "{topic}"),
                        Etags.of(id.format(), "absent"),
                        EtagBasis.CONFIGURATION,
                        PreconditionEnforcement.CHECKED_BEFORE);
            }
            case Operation.Update update -> {
                TopicDescription topic = described(context, id);
                refuseAssignment(update.patch());
                long now = topic.partitions().size();
                Optional<Long> partitions = Attrs.number(update.patch(), KafkaKinds.PARTITIONS);
                if (partitions.isPresent() && partitions.get() < now) {
                    refuseShrink(now, partitions.get());
                }
                if (partitions.isPresent() && partitions.get() > now) {
                    increase(context, id, partitions.get().intValue(), true);
                }
                Resource current = find(context, id).orElseThrow(() -> Handler.gone(id));
                Attributes.Builder before = Attributes.builder();
                update.patch()
                        .asMap()
                        .keySet()
                        .forEach(k -> current.attributes().raw(k).ifPresent(v -> before.putRaw(k, v)));
                List<AttributeChange> diff = new ArrayList<>(Attrs.diff(before.build(), update.patch()));
                if (partitions.isPresent() && partitions.get() > now) {
                    diff.add(new AttributeChange(KafkaKinds.NOTE, Optional.empty(), Optional.of(INCREASE_NOTE)));
                }
                return new Preview(
                        diff,
                        Set.of(),
                        new Estimate(Count.exact(1), "{topic}"),
                        etag(context, id, operation, scope),
                        EtagBasis.INCARNATION,
                        PreconditionEnforcement.CHECKED_BEFORE);
            }
            case Operation.Delete delete -> {
                TopicDescription topic = described(context, id);
                refuseIfInternal(topic);
                long records = records(context, id.name());
                if (delete.ifEmpty() && records > 0) {
                    refuseNotEmpty(id, records);
                }
                List<AttributeChange> diff = new ArrayList<>();
                diff.add(new AttributeChange(
                        KafkaKinds.PARTITIONS,
                        Optional.of((long) topic.partitions().size()),
                        Optional.empty()));
                diff.add(new AttributeChange(
                        KafkaKinds.REPLICATION_FACTOR, Optional.of(replicationFactor(topic)), Optional.empty()));
                groupsHolding(context, id.name())
                        .forEach(group -> diff.add(new AttributeChange(
                                KafkaKinds.OFFSETS_KEPT + group, Optional.empty(), Optional.of("offsets remain"))));
                diff.add(new AttributeChange(KafkaKinds.NOTE, Optional.empty(), Optional.of(DELETE_NOTE)));
                return new Preview(
                        diff,
                        Set.of(),
                        new Estimate(Count.estimate(records), "{message}"),
                        etag(context, id, operation, scope),
                        EtagBasis.INCARNATION,
                        PreconditionEnforcement.CHECKED_BEFORE);
            }
            case Operation.Truncate truncate -> {
                return Truncation.preview(context, id, id.name(), Optional.empty(), scope, truncate.position());
            }
            default -> throw unsupported(operation);
        }
    }

    @Override
    public String etag(Context context, ResourceId id, Operation operation, Scope scope) {
        if (operation instanceof Operation.Truncate) {
            return Truncation.etag(context, id, id.name(), Optional.empty(), scope);
        }
        if (operation instanceof Operation.Delete || increases(operation)) {
            TopicDescription topic = described(context, id);
            return Etags.of(id.format(), topic.topicId(), topic.partitions().size());
        }
        return Handler.super.etag(context, id, operation, scope);
    }

    @Override
    public OperationResult execute(Context context, ResourceId id, Operation operation, Scope scope) {
        return switch (operation) {
            case Operation.Create create -> create(context, id, create.attributes());
            case Operation.Update update -> update(context, id, update.patch());
            case Operation.Delete delete -> delete(context, id, delete);
            case Operation.Truncate truncate ->
                Truncation.execute(context, id.name(), Optional.empty(), scope, truncate.position());
            default -> throw unsupported(operation);
        };
    }

    static OperationResult create(Context context, ResourceId id, Attributes attributes) {
        NewTopic topic = newTopic(context, id, attributes);
        try {
            context.await()
                    .get(context.admin()
                            .createTopics(
                                    List.of(topic),
                                    new CreateTopicsOptions()
                                            .timeoutMs(context.await().timeoutMs()))
                            .all());
            Settle.until(context, () -> ready(context, id.name()));
        } catch (BrokerException e) {
            if (e.getCause() instanceof TopicExistsException) {
                return OperationResult.already();
            }
            throw e;
        } finally {
            context.topology().invalidate();
        }
        return OperationResult.done();
    }

    private static boolean ready(Context context, String topic) {
        if (gone(context, topic)) {
            return false;
        }
        TopicDescription description = describe(context, List.of(topic)).get(topic);
        if (description.partitions().stream()
                .anyMatch(p -> p.leader() == null || p.leader().isEmpty())) {
            return false;
        }
        Offsets.latest(
                context,
                description.partitions().stream()
                        .map(p -> new TopicPartition(topic, p.partition()))
                        .toList());
        return true;
    }

    private static boolean gone(Context context, String topic) {
        return !context.await()
                .get(context.admin()
                        .listTopics(new ListTopicsOptions()
                                .listInternal(true)
                                .timeoutMs(context.await().timeoutMs()))
                        .names())
                .contains(topic);
    }

    /** Whether the topic shows the partitions and settings an update asked for. */
    private static boolean shows(Context context, String topic, Optional<Long> partitions, List<AlterConfigOp> ops) {
        TopicDescription description = describe(context, List.of(topic)).get(topic);
        if (partitions.isPresent() && description.partitions().size() != partitions.get()) {
            return false;
        }
        if (ops.isEmpty()) {
            return true;
        }
        Config config = configs(context, Set.of(topic)).get(topic);
        for (AlterConfigOp op : ops) {
            ConfigEntry entry = config.get(op.configEntry().name());
            boolean overridden = entry != null && entry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;
            boolean agrees = op.opType() == AlterConfigOp.OpType.SET
                    ? entry != null && op.configEntry().value().equals(entry.value())
                    : !overridden;
            if (!agrees) {
                return false;
            }
        }
        return true;
    }

    private OperationResult update(Context context, ResourceId id, Attributes patch) {
        refuseAssignment(patch);
        Resource current = find(context, id).orElseThrow(() -> Handler.gone(id));
        List<AlterConfigOp> ops = new ArrayList<>();
        Optional<Long> partitions = Optional.empty();
        for (Map.Entry<String, Object> change : patch.asMap().entrySet()) {
            String key = change.getKey();
            if (key.equals(KafkaKinds.PARTITIONS)) {
                partitions = Attrs.number(patch, key);
            } else if (key.startsWith(KafkaKinds.CONFIG_PREFIX)) {
                String name = key.substring(KafkaKinds.CONFIG_PREFIX.length());
                String value = String.valueOf(change.getValue());
                ops.add(
                        value.isEmpty()
                                ? new AlterConfigOp(new ConfigEntry(name, null), AlterConfigOp.OpType.DELETE)
                                : new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET));
            } else if (key.startsWith("openbroker.")) {
                continue;
            } else {
                throw KafkaErrors.refused(
                        Status.INVALID_ARGUMENT,
                        key.equals(KafkaKinds.REPLICATION_FACTOR)
                                ? ErrorReasons.IMMUTABLE_FIELD
                                : ErrorReasons.INVALID_ARGUMENT,
                        "A topic's " + key + " cannot be changed this way.");
            }
        }
        long now = (Long) current.attributes().raw(KafkaKinds.PARTITIONS).orElse(0L);
        if (partitions.isPresent() && partitions.get() < now) {
            refuseShrink(now, partitions.get());
        }
        boolean changed = false;
        try {
            if (partitions.isPresent() && partitions.get() > now) {
                increase(context, id, partitions.get().intValue(), false);
                changed = true;
            }
            if (!ops.isEmpty()) {
                context.await()
                        .get(context.admin()
                                .incrementalAlterConfigs(
                                        Map.of(new ConfigResource(ConfigResource.Type.TOPIC, id.name()), ops),
                                        new AlterConfigsOptions()
                                                .timeoutMs(context.await().timeoutMs()))
                                .all());
                changed = true;
            }
            if (changed) {
                Optional<Long> target = partitions;
                Settle.until(context, () -> shows(context, id.name(), target, ops));
            }
        } finally {
            context.topology().invalidate();
        }
        return changed ? OperationResult.done() : OperationResult.already();
    }

    private OperationResult delete(Context context, ResourceId id, Operation.Delete delete) {
        TopicDescription topic = described(context, id);
        refuseIfInternal(topic);
        if (delete.ifEmpty()) {
            long records = records(context, id.name());
            if (records > 0) {
                refuseNotEmpty(id, records);
            }
        }
        try {
            context.await()
                    .get(context.admin()
                            .deleteTopics(
                                    TopicCollection.ofTopicNames(List.of(id.name())),
                                    new DeleteTopicsOptions()
                                            .timeoutMs(context.await().timeoutMs()))
                            .all());
            Settle.until(context, () -> gone(context, id.name()));
        } finally {
            context.topology().invalidate();
        }
        return OperationResult.done();
    }

    /** The topic, or a not-found refusal when the broker no longer has it. */
    private static TopicDescription described(Context context, ResourceId id) {
        TopicDescription topic = describe(context, List.of(id.name())).get(id.name());
        if (topic == null) {
            throw Handler.gone(id);
        }
        return topic;
    }

    private static boolean increases(Operation operation) {
        return operation instanceof Operation.Update update
                && update.patch().asMap().containsKey(KafkaKinds.PARTITIONS);
    }

    /** The settings the topic kind declares. A create that names another is refused before the broker is asked. */
    private static final Set<String> DECLARED_SETTINGS = Set.of(
            KafkaKinds.CONFIG_PREFIX + "cleanup.policy",
            KafkaKinds.CONFIG_PREFIX + "retention.ms",
            KafkaKinds.CONFIG_PREFIX + "retention.bytes",
            KafkaKinds.CONFIG_PREFIX + "max.message.bytes",
            KafkaKinds.CONFIG_PREFIX + "min.insync.replicas");

    private static void checkCreate(Context context, ResourceId id, Attributes attributes) {
        newTopic(context, id, attributes);
    }

    private static NewTopic newTopic(Context context, ResourceId id, Attributes attributes) {
        if (id.name().startsWith(INTERNAL_PREFIX)) {
            throw internal();
        }
        Optional<Integer> partitions = Attrs.number(attributes, KafkaKinds.PARTITIONS)
                .map(count -> {
                    if (count < 1 || count > Integer.MAX_VALUE) {
                        throw Attrs.invalid("A topic needs at least 1 partition, not " + count + ".");
                    }
                    return count.intValue();
                });
        Optional<Short> replication = Attrs.number(attributes, KafkaKinds.REPLICATION_FACTOR)
                .map(factor -> {
                    int brokers = brokersReached(context);
                    if (factor < 1 || factor > brokers) {
                        throw Attrs.invalid("Replication factor " + factor + " is outside 1.." + brokers
                                + ", and this connection reached " + brokers + " brokers.");
                    }
                    return factor.shortValue();
                });
        Map<String, String> configs = new LinkedHashMap<>();
        attributes.asMap().forEach((key, value) -> {
            if (key.startsWith(KafkaKinds.CONFIG_PREFIX)) {
                if (!DECLARED_SETTINGS.contains(key)) {
                    throw Attrs.invalid(
                            "A topic has no setting " + key.substring(KafkaKinds.CONFIG_PREFIX.length()) + ".");
                }
                configs.put(key.substring(KafkaKinds.CONFIG_PREFIX.length()), String.valueOf(value));
            } else if (!key.equals(KafkaKinds.PARTITIONS) && !key.equals(KafkaKinds.REPLICATION_FACTOR)) {
                if (!key.startsWith("openbroker.")) {
                    throw Attrs.invalid("A topic has no attribute " + key + ".");
                }
            }
        });
        return new NewTopic(id.name(), partitions, replication).configs(configs);
    }

    /** Asks the broker to accept the topic and creates nothing. */
    private static void validateNewTopic(Context context, ResourceId id, Attributes attributes) {
        try {
            context.await()
                    .get(context.admin()
                            .createTopics(
                                    List.of(newTopic(context, id, attributes)),
                                    new CreateTopicsOptions()
                                            .validateOnly(true)
                                            .timeoutMs(context.await().timeoutMs()))
                            .all());
        } catch (BrokerException e) {
            if (e.getCause() instanceof TopicExistsException) {
                throw KafkaErrors.refused(Status.ALREADY_EXISTS, ErrorReasons.RESOURCE_EXISTS, id + " already exists.");
            }
            throw e;
        }
    }

    private static int brokersReached(Context context) {
        return context.await()
                .get(context.admin()
                        .describeCluster(new DescribeClusterOptions()
                                .timeoutMs(context.await().timeoutMs()))
                        .nodes())
                .size();
    }

    private static void refuseIfInternal(TopicDescription topic) {
        if (topic.isInternal() || topic.name().startsWith(INTERNAL_PREFIX)) {
            throw internal();
        }
    }

    private static BrokerException internal() {
        return KafkaErrors.refused(Status.FAILED_PRECONDITION, ErrorReasons.PRECONDITION_FAILED, INTERNAL_REASON);
    }

    private static void refuseShrink(long now, long asked) {
        throw KafkaErrors.refused(
                Status.INVALID_ARGUMENT,
                ErrorReasons.IMMUTABLE_FIELD,
                "A topic's partitions can only be increased: it has " + now + ", not " + asked + ".");
    }

    private static void refuseAssignment(Attributes patch) {
        if (patch.asMap().containsKey(KafkaKinds.REPLICAS)) {
            throw KafkaErrors.refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    "A partition increase does not assign replicas.");
        }
    }

    private static void refuseNotEmpty(ResourceId id, long records) {
        throw KafkaErrors.refused(
                Status.FAILED_PRECONDITION,
                ErrorReasons.NOT_EMPTY,
                id + " still holds an estimate of " + records + " records.");
    }

    private static long replicationFactor(TopicDescription topic) {
        return topic.partitions().isEmpty()
                ? 0L
                : topic.partitions().getFirst().replicas().size();
    }

    /** End offset minus log start, summed. Compaction and transaction markers can overcount. */
    private static long records(Context context, String topic) {
        List<TopicPartition> partitions = Offsets.partitions(context, topic);
        Map<TopicPartition, Long> start = Offsets.earliest(context, partitions);
        Map<TopicPartition, Long> end = Offsets.latest(context, partitions);
        long total = 0;
        for (TopicPartition partition : partitions) {
            Long first = start.get(partition);
            Long last = end.get(partition);
            if (first == null || last == null) {
                throw KafkaErrors.refused(
                        Status.UNAVAILABLE,
                        ErrorReasons.UNREACHABLE,
                        "The record estimate for " + topic + " could not be read.");
            }
            total += Math.max(0, last - first);
        }
        return total;
    }

    /** Groups that have committed or start offsets on the topic. A short read fails the preview. */
    private static Set<String> groupsHolding(Context context, String topic) {
        List<TopicPartition> partitions = Offsets.partitions(context, topic);
        if (partitions.isEmpty()) {
            return Set.of();
        }
        Map<String, ListConsumerGroupOffsetsSpec> consumers = new LinkedHashMap<>();
        Map<String, ListShareGroupOffsetsSpec> shares = new LinkedHashMap<>();
        Map<String, ListStreamsGroupOffsetsSpec> streams = new LinkedHashMap<>();
        for (GroupListing group : GroupInfo.list(context)) {
            switch (GroupInfo.typeOf(group)) {
                case SHARE -> shares.put(group.groupId(), new ListShareGroupOffsetsSpec().topicPartitions(partitions));
                case STREAMS ->
                    streams.put(group.groupId(), new ListStreamsGroupOffsetsSpec().topicPartitions(partitions));
                default ->
                    consumers.put(group.groupId(), new ListConsumerGroupOffsetsSpec().topicPartitions(partitions));
            }
        }
        Set<String> named = new TreeSet<>();
        int timeout = context.await().timeoutMs();
        if (!consumers.isEmpty()) {
            context.await()
                    .get(context.admin()
                            .listConsumerGroupOffsets(
                                    consumers, new ListConsumerGroupOffsetsOptions().timeoutMs(timeout))
                            .all())
                    .forEach((group, offsets) -> keep(named, group, topic, offsets.keySet()));
        }
        if (!streams.isEmpty()) {
            context.await()
                    .get(context.admin()
                            .listStreamsGroupOffsets(streams, new ListStreamsGroupOffsetsOptions().timeoutMs(timeout))
                            .all())
                    .forEach((group, offsets) -> keep(named, group, topic, offsets.keySet()));
        }
        if (!shares.isEmpty()) {
            context.await()
                    .get(context.admin()
                            .listShareGroupOffsets(shares, new ListShareGroupOffsetsOptions().timeoutMs(timeout))
                            .all())
                    .forEach((group, offsets) -> keep(named, group, topic, offsets.keySet()));
        }
        return named;
    }

    private static void keep(Set<String> named, String group, String topic, Set<TopicPartition> partitions) {
        if (partitions.stream().anyMatch(partition -> topic.equals(partition.topic()))) {
            named.add(group);
        }
    }

    private static void increase(Context context, ResourceId id, int partitions, boolean validateOnly) {
        context.await()
                .get(context.admin()
                        .createPartitions(
                                Map.of(id.name(), NewPartitions.increaseTo(partitions)),
                                new CreatePartitionsOptions()
                                        .validateOnly(validateOnly)
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
    }
}
