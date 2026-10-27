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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TerminateTransactionOptions;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.TopicPartition;

/** Transactional ids: the producers that write in transactions, and the transactions they leave open. */
final class TransactionalIds implements Handler {

    private final Kind kind;

    TransactionalIds(Kind kind) {
        this.kind = kind;
    }

    @Override
    public Kind kind() {
        return kind;
    }

    @Override
    public Set<String> selectors() {
        return Set.of("state");
    }

    static ResourceId id(String transactionalId) {
        return new ResourceId(KafkaKinds.TRANSACTIONAL_ID, ResourcePath.of(transactionalId));
    }

    private static List<TransactionListing> list(Context context) {
        return List.copyOf(context.await()
                .get(context.admin()
                        .listTransactions(new ListTransactionsOptions()
                                .timeoutMs(context.await().timeoutMs()))
                        .all()));
    }

    @Override
    public List<Candidate> candidates(Context context, Listing listing) {
        var match = listing.names();
        Optional<String> state = listing.field("state");
        List<Candidate> out = new ArrayList<>();
        for (TransactionListing t : list(context)) {
            String name = t.state().toString().toLowerCase(Locale.ROOT);
            if (match.test(t.transactionalId())
                    && state.map(name::equalsIgnoreCase).orElse(true)) {
                out.add(new Candidate(
                        id(t.transactionalId()),
                        List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                        Attributes.builder()
                                .putRaw(KafkaKinds.PRODUCER_ID, t.producerId())
                                .putRaw(KafkaKinds.TRANSACTION_STATE, name)
                                .build()));
            }
        }
        out.sort(Comparator.comparing(c -> c.id().format()));
        return out;
    }

    @Override
    public List<Resource> resources(Context context, List<Candidate> page, Listing listing) {
        if (page.isEmpty()) {
            return List.of();
        }
        List<String> ids = page.stream().map(c -> c.id().name()).toList();
        Map<String, TransactionDescription> described = context.await()
                .get(context.admin()
                        .describeTransactions(
                                ids,
                                new DescribeTransactionsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
        List<Resource> out = new ArrayList<>();
        for (Candidate candidate : page) {
            TransactionDescription d = described.get(candidate.id().name());
            Attributes.Builder attributes = Attributes.builder().putAll(candidate.attributes());
            if (d != null) {
                attributes
                        .putRaw(KafkaKinds.PRODUCER_ID, d.producerId())
                        .putRaw(KafkaKinds.PRODUCER_EPOCH, (long) d.producerEpoch())
                        .putRaw(
                                KafkaKinds.TRANSACTION_STATE,
                                d.state().toString().toLowerCase(Locale.ROOT))
                        .putRaw(KafkaKinds.TRANSACTION_TIMEOUT_MS, d.transactionTimeoutMs())
                        .putRaw(KafkaKinds.COORDINATOR, (long) d.coordinatorId())
                        .putRaw(
                                KafkaKinds.TRANSACTION_PARTITIONS,
                                d.topicPartitions().stream()
                                        .map(TopicPartition::toString)
                                        .sorted()
                                        .toList());
                d.transactionStartTimeMs()
                        .ifPresent(ms -> attributes.putRaw(KafkaKinds.TRANSACTION_START, Instant.ofEpochMilli(ms)));
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

    @Override
    public Optional<Resource> find(Context context, ResourceId id) {
        Listing everything = new Listing(ResourceQuery.of(KafkaKinds.TRANSACTIONAL_ID));
        List<Candidate> found = candidates(context, everything).stream()
                .filter(c -> c.id().equals(id))
                .toList();
        return resources(context, found, everything).stream().findFirst();
    }

    @Override
    public Preview preview(Context context, ResourceId id, Operation operation, Scope scope) {
        if (!(operation instanceof Operation.Resolve resolve)) {
            throw unsupported(operation);
        }
        requireRollback(resolve);
        Resource current = find(context, id).orElseThrow(() -> Handler.gone(id));
        Attributes after = Attributes.builder()
                .putRaw(KafkaKinds.TRANSACTION_STATE, "aborted")
                .build();
        return new Preview(
                Attrs.diff(
                        Attributes.builder()
                                .putRaw(
                                        KafkaKinds.TRANSACTION_STATE,
                                        current.attributes()
                                                .raw(KafkaKinds.TRANSACTION_STATE)
                                                .orElse("unknown"))
                                .build(),
                        after),
                Set.of(),
                new Estimate(Count.exact(1), "{transaction}"),
                etag(context, id, operation, scope),
                PreconditionEnforcement.CHECKED_BEFORE);
    }

    @Override
    public String etag(Context context, ResourceId id, Operation operation, Scope scope) {
        Resource current = find(context, id).orElseThrow(() -> Handler.gone(id));
        Attributes a = current.attributes();
        return Etags.of(
                id.format(),
                a.raw(KafkaKinds.PRODUCER_ID).orElse(""),
                a.raw(KafkaKinds.PRODUCER_EPOCH).orElse(""),
                a.raw(KafkaKinds.TRANSACTION_STATE).orElse(""),
                a.raw(KafkaKinds.TRANSACTION_START).orElse(""));
    }

    private static void requireRollback(Operation.Resolve resolve) {
        if (resolve.resolution() != Resolution.ROLLBACK) {
            throw KafkaErrors.refused(
                    Status.UNIMPLEMENTED,
                    ErrorReasons.NOT_SUPPORTED,
                    "Kafka can abort a transaction another producer left open, not commit it: the records belong to the"
                            + " producer that wrote them.");
        }
    }

    @Override
    public OperationResult execute(Context context, ResourceId id, Operation operation, Scope scope) {
        if (!(operation instanceof Operation.Resolve resolve)) {
            throw unsupported(operation);
        }
        requireRollback(resolve);
        Resource current = find(context, id).orElseThrow(() -> Handler.gone(id));
        String state = String.valueOf(
                current.attributes().raw(KafkaKinds.TRANSACTION_STATE).orElse(""));
        if (!state.equals("ongoing") && !state.startsWith("prepare")) {
            return OperationResult.already();
        }
        try {
            context.await()
                    .get(context.admin()
                            .forceTerminateTransaction(
                                    id.name(),
                                    new TerminateTransactionOptions()
                                            .timeoutMs(context.await().timeoutMs()))
                            .result());
        } catch (BrokerException e) {
            if (!e.reason().equals(ErrorReasons.UNSUPPORTED_VERSION)) {
                throw e;
            }
            abortPartitions(context, current);
        }
        return OperationResult.done();
    }

    /** Aborts, partition by partition, what the cluster's coordinator cannot terminate in one request. */
    private static void abortPartitions(Context context, Resource transaction) {
        long producerId =
                (Long) transaction.attributes().raw(KafkaKinds.PRODUCER_ID).orElseThrow();
        @SuppressWarnings("unchecked")
        List<String> partitions = (List<String>)
                transaction.attributes().raw(KafkaKinds.TRANSACTION_PARTITIONS).orElse(List.of());
        List<TopicPartition> touched = new ArrayList<>();
        for (String text : partitions) {
            int dash = text.lastIndexOf('-');
            touched.add(new TopicPartition(text.substring(0, dash), Integer.parseInt(text.substring(dash + 1))));
        }
        var producers = context.await()
                .get(context.admin()
                        .describeProducers(
                                touched,
                                new DescribeProducersOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
        Map<TopicPartition, ProducerState> open = new HashMap<>();
        producers.forEach((tp, state) -> state.activeProducers().stream()
                .filter(p -> p.producerId() == producerId
                        && p.currentTransactionStartOffset().isPresent())
                .findFirst()
                .ifPresent(p -> open.put(tp, p)));
        for (Map.Entry<TopicPartition, ProducerState> e : open.entrySet()) {
            ProducerState p = e.getValue();
            context.await()
                    .get(context.admin()
                            .abortTransaction(new AbortTransactionSpec(
                                    e.getKey(),
                                    p.producerId(),
                                    (short) p.producerEpoch(),
                                    p.coordinatorEpoch().orElse(0)))
                            .all());
        }
    }
}
