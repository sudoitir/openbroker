package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * The handler of each kind, and which kinds this cluster has. The share-group kind is declared only
 * where the cluster runs share groups in production; that is read once and kept until the
 * connection is pointed at another cluster.
 */
final class Handlers {

    private final KafkaCluster cluster;
    private final Topology topology;
    private final Map<KindId, Handler> base = new LinkedHashMap<>();
    private final Handler shares;

    Handlers(KafkaCluster cluster, Topology topology) {
        this.cluster = cluster;
        this.topology = topology;
        for (Kind kind : KafkaKinds.base()) {
            Handler handler =
                    switch (kind.id().value()) {
                        case "kafka.topic" -> new Topics(kind);
                        case "kafka.partition" -> new Partitions(kind);
                        case "kafka.group" -> new Groups(kind);
                        case "kafka.group-topic" -> new ConsumerSubscriptions(kind);
                        case "kafka.acl" -> new Acls(kind);
                        case "kafka.hanging-transaction" -> new HangingTransactions(kind);
                        default -> new TransactionalIds(kind);
                    };
            base.put(kind.id(), handler);
        }
        this.shares = new ShareSubscriptions(KafkaKinds.shareGroupTopic());
    }

    /** Forgets what was read from the cluster, because the connection now points at another. */
    void forget() {
        topology.invalidate();
    }

    /** Runs {@code work} on one call to the cluster, with a context for the handlers. */
    <T> T call(String operation, Optional<ResourceId> target, Deadline deadline, Function<Context, T> work) {
        return cluster.call(
                operation,
                target,
                deadline,
                (admin, await) -> work.apply(new Context(
                        cluster,
                        topology,
                        admin,
                        await,
                        cluster.options().clock().instant(),
                        null)));
    }

    /** Like {@link #call}, for a call that reads several queries, which share what they ask of the cluster. */
    <T> T callReading(String operation, Deadline deadline, Function<Context, T> work) {
        return cluster.call(
                operation,
                Optional.empty(),
                deadline,
                (admin, await) -> work.apply(new Context(
                        cluster,
                        topology,
                        admin,
                        await,
                        cluster.options().clock().instant(),
                        new Reads())));
    }

    /** Like {@link #call}, on another plane's credentials and permission. */
    <T> T callAs(
            String operation, Plane plane, Optional<ResourceId> target, Deadline deadline, Function<Context, T> work) {
        return cluster.guarded(
                operation,
                plane,
                target,
                deadline,
                () -> cluster.withAdmin(
                        deadline,
                        (admin, await) -> work.apply(new Context(
                                cluster,
                                topology,
                                admin,
                                await,
                                cluster.options().clock().instant(),
                                null))));
    }

    private boolean sharesInProduction(Context context) {
        return cluster.features(context.admin(), context.await()).share().available();
    }

    /** Whether the cluster lists transactions, which Kafka 3.0 added (KIP-664). */
    private boolean listsTransactions(Context context) {
        return cluster.features(context.admin(), context.await()).transactions();
    }

    /** The kinds of the cluster the context is on. */
    List<Kind> kinds(Context context) {
        List<Kind> out = new ArrayList<>();
        if (listsTransactions(context)) {
            base.values().forEach(h -> out.add(h.kind()));
        } else {
            // A cluster before 3.0 has transactional ids but cannot list transactions or the producers in them.
            base.values().stream()
                    .filter(h -> !h.kind().id().equals(KafkaKinds.HANGING_TRANSACTION))
                    .forEach(h -> out.add(h.kind()));
        }
        if (sharesInProduction(context)) {
            out.add(shares.kind());
        }
        return out;
    }

    /** The handler of a kind, which the cluster must have. */
    Handler of(Context context, KindId kind) {
        Handler handler = base.get(kind);
        if (handler != null) {
            if (kind.equals(KafkaKinds.HANGING_TRANSACTION) && !listsTransactions(context)) {
                throw KafkaErrors.refused(
                        Status.FAILED_PRECONDITION,
                        ErrorReasons.NOT_SUPPORTED,
                        "This cluster cannot list hanging transactions: finding one needs Kafka 3.0 or later"
                                + " (KIP-664), which lists the producers in a partition.");
            }
            return handler;
        }
        if (kind.equals(shares.kind().id())) {
            if (sharesInProduction(context)) {
                return shares;
            }
            throw KafkaErrors.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.NOT_SUPPORTED,
                    "This cluster does not run share groups as a production feature: it needs Kafka 4.2 or later with"
                            + " share.version finalized at 1 or more.");
        }
        throw KafkaErrors.refused(
                Status.INVALID_ARGUMENT, ErrorReasons.INVALID_ARGUMENT, "Kafka has no kind " + kind + ".");
    }

    /** The handlers of the kinds of a class that the cluster has. */
    List<Handler> ofClass(Context context, ResourceClass resourceClass) {
        return kinds(context).stream()
                .filter(k -> k.resourceClass() == resourceClass)
                .map(k -> of(context, k.id()))
                .toList();
    }
}
