package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import io.github.sudoitir.openbroker.operation.Resolution;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

/**
 * Transactions that hang: open on a partition, past the longest a transaction may last, and unknown to
 * the coordinator, which would otherwise abort them within seconds. Such a transaction holds the
 * partition's last stable offset back for ever, so a {@code read_committed} consumer stalls behind it.
 * They are found as Kafka's {@code kafka-transactions.sh find-hanging} does: the producers of every
 * partition (one request per broker, not per partition) that have an open transaction older than
 * {@code transaction.max.timeout.ms}, less those the coordinator still lists as ongoing. Listing reads
 * every partition of the topics in scope, so it is made on demand and never on a watch.
 */
record HangingTransactions(Kind kind) implements Handler {

    /** The longest a transaction may last where the broker does not say, Kafka's default. */
    static final long DEFAULT_MAX_TIMEOUT_MS = 900_000;

    /** The most hanging transactions a listing reports when the query sets no limit. */
    static final int DEFAULT_LIMIT = 1000;

    private static final String MAX_TIMEOUT = "transaction.max.timeout.ms";

    @Override
    public Set<String> selectors() {
        return Set.of("topic");
    }

    /** A producer's open transaction on a partition. */
    record Open(TopicPartition partition, ProducerState producer) {}

    /**
     * Whether an open transaction hangs: the producer wrote last longer ago than any transaction may last,
     * and the coordinator does not list the producer's transaction as in progress.
     */
    static boolean hangs(ProducerState producer, long nowMs, long maxTimeoutMs, Set<Long> inProgressAtCoordinator) {
        return producer.currentTransactionStartOffset().isPresent()
                && nowMs - producer.lastTimestamp() > maxTimeoutMs
                && !inProgressAtCoordinator.contains(producer.producerId());
    }

    static ResourceId id(TopicPartition partition, long producerId) {
        return new ResourceId(
                KafkaKinds.HANGING_TRANSACTION,
                ResourcePath.of(partition.topic(), String.valueOf(partition.partition()), String.valueOf(producerId)));
    }

    private static TopicPartition partitionOf(ResourceId id) {
        List<String> parts = id.path().segments();
        if (parts.size() != 3) {
            throw Attrs.invalid("A hanging transaction is [topic, partition, producerId]: " + id);
        }
        try {
            Long.parseLong(parts.get(2));
            return new TopicPartition(parts.get(0), Integer.parseInt(parts.get(1)));
        } catch (NumberFormatException _) {
            throw Attrs.invalid("A partition and a producer id are whole numbers: " + id);
        }
    }

    private static long producerOf(ResourceId id) {
        return Long.parseLong(id.path().segments().get(2));
    }

    /** The open transactions of the partitions' producers, one request to each broker that leads some. */
    private static List<Open> open(Context context, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return List.of();
        }
        var result = context.admin()
                .describeProducers(
                        partitions,
                        new DescribeProducersOptions().timeoutMs(context.await().timeoutMs()));
        List<Open> out = new ArrayList<>();
        for (TopicPartition partition : partitions) {
            try {
                for (ProducerState p :
                        context.await().get(result.partitionResult(partition)).activeProducers()) {
                    if (p.currentTransactionStartOffset().isPresent()) {
                        out.add(new Open(partition, p));
                    }
                }
            } catch (BrokerException e) {
                // A partition with no leader for the moment has no producers to read; a cluster or account
                // that cannot list producers at all fails every listing.
                if (e.reason().equals(ErrorReasons.UNSUPPORTED_VERSION) || e.status() == Status.PERMISSION_DENIED) {
                    throw e;
                }
            }
        }
        return out;
    }

    /** The producers whose transaction the coordinator lists as in progress. */
    private static Set<Long> inProgress(Context context, Collection<Long> producerIds) {
        if (producerIds.isEmpty()) {
            return Set.of();
        }
        Set<Long> out = new HashSet<>();
        for (TransactionListing t : context.await()
                .get(context.admin()
                        .listTransactions(new ListTransactionsOptions()
                                .filterProducerIds(producerIds)
                                .timeoutMs(context.await().timeoutMs()))
                        .all())) {
            if (t.state() == TransactionState.ONGOING
                    || t.state() == TransactionState.PREPARE_COMMIT
                    || t.state() == TransactionState.PREPARE_ABORT) {
                out.add(t.producerId());
            }
        }
        return out;
    }

    /** The longest a transaction may last, from the first broker's configuration, else Kafka's default. */
    private static long maxTimeoutMs(Context context) {
        try {
            var nodes = context.await()
                    .get(context.admin()
                            .describeCluster(new DescribeClusterOptions()
                                    .timeoutMs(context.await().timeoutMs()))
                            .nodes());
            if (nodes.isEmpty()) {
                return DEFAULT_MAX_TIMEOUT_MS;
            }
            ConfigResource broker = new ConfigResource(
                    ConfigResource.Type.BROKER, nodes.iterator().next().idString());
            Config config = context.await()
                    .get(context.admin()
                            .describeConfigs(
                                    List.of(broker),
                                    new DescribeConfigsOptions()
                                            .timeoutMs(context.await().timeoutMs()))
                            .all())
                    .get(broker);
            var entry = config == null ? null : config.get(MAX_TIMEOUT);
            return entry == null || entry.value() == null ? DEFAULT_MAX_TIMEOUT_MS : Long.parseLong(entry.value());
        } catch (BrokerException | NumberFormatException _) {
            return DEFAULT_MAX_TIMEOUT_MS;
        }
    }

    /** The transactions that hang on the partitions. */
    static List<Open> hanging(Context context, Collection<TopicPartition> partitions) {
        return hanging(context, partitions, producerIds -> inProgress(context, producerIds));
    }

    /** Like {@link #hanging(Context, Collection)}, with the coordinator's view of which producers are in progress. */
    static List<Open> hanging(
            Context context, Collection<TopicPartition> partitions, Function<Collection<Long>, Set<Long>> coordinator) {
        List<Open> open = open(context, partitions);
        if (open.isEmpty()) {
            return List.of();
        }
        long maxTimeout = maxTimeoutMs(context);
        Set<Long> inProgress = coordinator.apply(
                open.stream().map(o -> o.producer().producerId()).toList());
        long now = context.now().toEpochMilli();
        return open.stream()
                .filter(o -> hangs(o.producer(), now, maxTimeout, inProgress))
                .toList();
    }

    static Attributes attributes(Context context, Open o) {
        ProducerState p = o.producer();
        return Attributes.builder()
                .putRaw(KafkaKinds.HANGING_TOPIC, o.partition().topic())
                .putRaw(KafkaKinds.HANGING_PARTITION, (long) o.partition().partition())
                .putRaw(KafkaKinds.PRODUCER_ID, p.producerId())
                .putRaw(KafkaKinds.PRODUCER_EPOCH, (long) p.producerEpoch())
                .putRaw(KafkaKinds.COORDINATOR_EPOCH, (long)
                        p.coordinatorEpoch().orElse(-1))
                .putRaw(
                        KafkaKinds.TRANSACTION_START_OFFSET,
                        p.currentTransactionStartOffset().getAsLong())
                .putRaw(KafkaKinds.HANGING_AGE_MS, Math.max(0, context.now().toEpochMilli() - p.lastTimestamp()))
                .build();
    }

    @Override
    public List<Candidate> candidates(Context context, Listing listing) {
        Optional<String> topic = listing.field("topic");
        Collection<String> topics = topic.isPresent()
                ? List.of(topic.get())
                : new TreeSet<>(context.await()
                        .get(context.admin()
                                .listTopics(new ListTopicsOptions()
                                        .listInternal(false)
                                        .timeoutMs(context.await().timeoutMs()))
                                .names()));
        List<TopicPartition> partitions = Offsets.partitions(context, topics).values().stream()
                .flatMap(List::stream)
                .toList();
        int limit = listing.query().limit() > 0 ? listing.query().limit() : DEFAULT_LIMIT;
        List<Candidate> out = new ArrayList<>();
        for (Open o : hanging(context, partitions)) {
            out.add(new Candidate(
                    id(o.partition(), o.producer().producerId()),
                    List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                    attributes(context, o)));
        }
        out.sort(Comparator.comparing(c -> c.id().format()));
        return out.size() > limit ? out.subList(0, limit) : out;
    }

    @Override
    public List<Resource> resources(Context context, List<Candidate> page, Listing listing) {
        if (page.isEmpty()) {
            return List.of();
        }
        List<TopicPartition> partitions =
                page.stream().map(c -> partitionOf(c.id())).distinct().toList();
        Map<TopicPartition, Long> end = endOffsets(context, partitions);
        List<Resource> out = new ArrayList<>();
        for (Candidate candidate : page) {
            TopicPartition partition = partitionOf(candidate.id());
            Attributes.Builder attributes = Attributes.builder().putAll(candidate.attributes());
            Long last = end.get(partition);
            Long start = Attrs.number(candidate.attributes(), KafkaKinds.TRANSACTION_START_OFFSET)
                    .orElse(null);
            if (last != null && start != null) {
                attributes.putRaw(KafkaKinds.RECORDS_HELD_BACK, Math.max(0, last - start));
            }
            out.add(new Resource(
                    candidate.id(),
                    Attrs.project(attributes.build(), listing.query().projection()),
                    List.of(),
                    candidate.placements(),
                    context.now()));
        }
        return out;
    }

    /** The end of each log including what is not yet committed: where a held-back transaction's records stop. */
    private static Map<TopicPartition, Long> endOffsets(Context context, Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        partitions.forEach(p -> request.put(p, OffsetSpec.latest()));
        Map<TopicPartition, Long> out = new HashMap<>();
        context.await()
                .get(context.admin()
                        .listOffsets(
                                request,
                                new ListOffsetsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all())
                .forEach((p, info) -> out.put(p, info.offset()));
        return out;
    }

    @Override
    public Optional<Resource> find(Context context, ResourceId id) {
        TopicPartition partition = partitionOf(id);
        long producerId = producerOf(id);
        List<Candidate> found = hanging(context, List.of(partition)).stream()
                .filter(o -> o.producer().producerId() == producerId)
                .map(o -> new Candidate(
                        id(o.partition(), producerId),
                        List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                        attributes(context, o)))
                .toList();
        return resources(context, found, new Listing(ResourceQuery.of(kind.id()))).stream()
                .findFirst();
    }

    private static void requireRollback(Operation.Resolve resolve) {
        if (resolve.resolution() != Resolution.ROLLBACK) {
            throw KafkaErrors.refused(
                    Status.UNIMPLEMENTED,
                    ErrorReasons.NOT_SUPPORTED,
                    "Kafka can abort a transaction left open, not commit it: the records belong to the producer"
                            + " that wrote them.");
        }
    }

    private static BrokerException notOpen(ResourceId id) {
        return KafkaErrors.refused(
                Status.FAILED_PRECONDITION,
                ErrorReasons.PRECONDITION_FAILED,
                id + " is no longer open on its partition.");
    }

    @Override
    public Preview preview(Context context, ResourceId id, Operation operation, Scope scope) {
        if (!(operation instanceof Operation.Resolve resolve)) {
            throw unsupported(operation);
        }
        requireRollback(resolve);
        Resource current = find(context, id).orElseThrow(() -> notOpen(id));
        long held =
                Attrs.number(current.attributes(), KafkaKinds.RECORDS_HELD_BACK).orElse(0L);
        return new Preview(
                Attrs.diff(
                        Attributes.builder()
                                .putRaw(KafkaKinds.TRANSACTION_STATE, "open")
                                .build(),
                        Attributes.builder()
                                .putRaw(KafkaKinds.TRANSACTION_STATE, "aborted")
                                .build()),
                Set.of(),
                new Estimate(Count.exact(held), "{record}"),
                etag(context, id, operation, scope),
                PreconditionEnforcement.CHECKED_BEFORE);
    }

    @Override
    public String etag(Context context, ResourceId id, Operation operation, Scope scope) {
        Resource current = find(context, id).orElseThrow(() -> notOpen(id));
        Attributes a = current.attributes();
        return Etags.of(
                id.format(),
                a.raw(KafkaKinds.TRANSACTION_START_OFFSET).orElse(""),
                a.raw(KafkaKinds.PRODUCER_EPOCH).orElse(""));
    }

    @Override
    public OperationResult execute(Context context, ResourceId id, Operation operation, Scope scope) {
        if (!(operation instanceof Operation.Resolve resolve)) {
            throw unsupported(operation);
        }
        requireRollback(resolve);
        TopicPartition partition = partitionOf(id);
        long producerId = producerOf(id);
        ProducerState producer = open(context, List.of(partition)).stream()
                .map(Open::producer)
                .filter(p -> p.producerId() == producerId)
                .findFirst()
                .orElseThrow(() -> notOpen(id));
        context.await()
                .get(context.admin()
                        .abortTransaction(new AbortTransactionSpec(
                                partition,
                                producer.producerId(),
                                (short) producer.producerEpoch(),
                                producer.coordinatorEpoch().orElse(0)))
                        .all());
        return OperationResult.done();
    }
}
