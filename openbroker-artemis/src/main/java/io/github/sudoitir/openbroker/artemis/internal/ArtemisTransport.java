package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Remedy;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.artemis.ArtemisRemedies;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.MessageHandler;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.PrefetchBound;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.RetryMode;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.SettlementScope;
import io.github.sudoitir.openbroker.messaging.SubscriptionCapabilities;
import io.github.sudoitir.openbroker.messaging.SystemRetry;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.support.Deadlines;
import io.github.sudoitir.openbroker.support.messaging.ProcessorEngine;
import io.github.sudoitir.openbroker.support.messaging.Retries;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import org.apache.activemq.artemis.api.core.ActiveMQException;

/**
 * The messaging client over Core sessions the driver owns. Opening a link and committing are
 * admitted by the caller's interceptor; sending and receiving each message are not.
 *
 * <p>What it does on Artemis:
 *
 * <ul>
 *   <li>Senders are a {@link CoreSender} over the engine's send pipeline; receivers and the lanes of
 *       a processor are a {@link CoreReceiver} each, on a Core session of their own. A link that is
 *       lost is reopened by the driver, on its node and then on the serving member of its HA group,
 *       and its state changes reach {@link #watch}.
 *   <li>Retries. The default resolves to {@code SYSTEM}, because the broker counts attempts. With
 *       {@code RetryMode.SYSTEM} nothing is counted in the client: a failing handler
 *       releases its message counted, and the broker's address settings decide: {@code
 *       redelivery-delay} and its multiplier, {@code max-delivery-attempts} and {@code
 *       dead-letter-address}. With {@code MANAGED} the client counts, so delay, attempts and dead
 *       letter are all the client's: an ordered processor retries in place, an unordered one sends a
 *       delayed copy of the message to its queue, and a message is dead-lettered to the policy's
 *       target, or the address's dead-letter address, after the last attempt. {@code Reject} always
 *       dead-letters now.
 * </ul>
 */
public final class ArtemisTransport implements Transport, AutoCloseable {

    static final TransportCapabilities CAPABILITIES = new TransportCapabilities(
            Set.of(Guarantee.AT_MOST_ONCE, Guarantee.AT_LEAST_ONCE, Guarantee.TRANSACTED),
            DeduplicationScope.WINDOW,
            Set.of(OrderingScope.NONE, OrderingScope.GROUP, OrderingScope.SUBSCRIPTION),
            Set.of(PrefetchBound.BYTES),
            Map.of(
                    ArtemisKinds.QUEUE,
                    new SubscriptionCapabilities(
                            Set.of(OrderingScope.NONE, OrderingScope.GROUP, OrderingScope.SUBSCRIPTION),
                            SettlementScope.RECEIVER,
                            true,
                            new SystemRetry(true, true, true))),
            true);

    private final Cluster cluster;
    private final TransportEnv env;

    ArtemisTransport(Cluster cluster, CoreLinks links, ArtemisNodes nodes) {
        this.cluster = cluster;
        this.env = new TransportEnv(
                cluster,
                links,
                new HaGroups(cluster::nodes, () -> nodes.describe(Scope.whole()).value()),
                new Addresses(cluster));
    }

    @Override
    public TransportCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public AutoCloseable watch(LinkListener listener) {
        return env.watch(listener);
    }

    private static void requireDeclared(
            Guarantee guarantee,
            OrderingScope ordering,
            Set<OrderingScope> orderings,
            DeduplicationScope deduplication) {
        if (!CAPABILITIES.deduplication().satisfies(deduplication)) {
            throw Unimplemented.of("deduplicate in the scope " + deduplication);
        }
        if (!CAPABILITIES.guarantees().contains(guarantee)) {
            throw Unimplemented.of("open a link with the guarantee " + guarantee);
        }
        if (!orderings.contains(ordering)) {
            throw Unimplemented.of("keep " + ordering + " order: Artemis has no partitions");
        }
    }

    private NodeHandle single(Scope scope, String what) {
        List<NodeHandle> nodes = cluster.nodes(scope);
        if (nodes.size() != 1) {
            throw Queries.invalid("A " + what + " reads one node; scope it to one node instance.");
        }
        return env.groups().serving(nodes.getFirst());
    }

    @Override
    public Receiver receiver(ResourceId subscription, Scope scope, ReceiverOptions options) {
        Targets.requireQueue(subscription);
        requireDeclared(options.guarantee(), OrderingScope.NONE, Set.of(OrderingScope.NONE), DeduplicationScope.NONE);
        NodeHandle node = single(scope, "receiver");
        Deadline deadline = Deadlines.current(cluster.options());
        String selector = Selectors.bounded(Selectors.of(options.filter()), options.producedAtOrBefore());
        String destination = Destinations.of(cluster, node, subscription, deadline);
        return opened(node, "receiver.open", subscription, () -> {
            env.groups().learn(node);
            return env.track(CoreReceiver.open(
                    env,
                    UUID.randomUUID().toString(),
                    node,
                    subscription,
                    destination,
                    selector,
                    options,
                    options.flow().prefetchBytes(),
                    Optional::empty));
        });
    }

    @Override
    public Sender sender(SendTarget target, Scope scope, SenderOptions options) {
        requireDeclared(options.guarantee(), options.ordering(), CAPABILITIES.orderings(), options.deduplication());
        Deadline deadline = Deadlines.current(cluster.options());
        BrokerException last = null;
        for (NodeHandle node : cluster.nodes(scope)) {
            try {
                Targets.Resolved resolved = Targets.resolve(cluster, node, target, deadline);
                return opened(node, "sender.open", resolved.resource(), () -> {
                    env.groups().learn(node);
                    return env.track(CoreSender.open(
                            env,
                            node,
                            resolved.resource(),
                            resolved.subscription(),
                            resolved.destination(),
                            resolved.address(),
                            options));
                });
            } catch (BrokerException e) {
                if (!ErrorReasons.RESOURCE_GONE.equals(e.reason())) {
                    throw e;
                }
                last = e;
            }
        }
        throw last != null ? last : Queries.invalid("The scope names no node.");
    }

    @Override
    public Processor process(ResourceId subscription, Scope scope, ProcessorOptions asked, MessageHandler handler) {
        Targets.requireQueue(subscription);
        SubscriptionCapabilities queue = CAPABILITIES.subscription(subscription.kind());
        ProcessorOptions options = asked.withRetry(Retries.resolve(asked.retry(), queue.retry()));
        requireDeclared(options.receive().guarantee(), options.ordering(), queue.orderings(), DeduplicationScope.NONE);
        NodeHandle node = single(scope, "processor");
        Deadline deadline = Deadlines.current(cluster.options());
        String selector = Selectors.bounded(
                Selectors.of(options.receive().filter()), options.receive().producedAtOrBefore());
        String destination = Destinations.of(cluster, node, subscription, deadline);
        requireRetryTheBrokerAllows(node, Targets.addressOf(destination), options.retry());
        String id = UUID.randomUUID().toString();
        CoreLanes lanes = new CoreLanes(
                env,
                id,
                node,
                subscription,
                destination,
                selector,
                options,
                dead -> sender(dead, scope, SenderOptions.defaults()));
        OperationInfo info = env.info(OperationType.PROCESS, subscription, Optional.of(subscription), node, 1);
        return opened(node, "processor.open", subscription, () -> {
            env.groups().learn(node);
            try {
                return env.track(new CoreProcessor(
                        env, ProcessorEngine.start(id, options, info, env.engine(), lanes, handler), lanes));
            } catch (RuntimeException e) {
                lanes.close();
                throw e;
            }
        });
    }

    /**
     * A managed retry that allows more attempts than the broker delivers before it dead-letters
     * would never reach its last attempt.
     */
    private void requireRetryTheBrokerAllows(NodeHandle node, String address, RetryPolicy retry) {
        if (retry.mode() != RetryMode.MANAGED) {
            return;
        }
        OptionalInt limit = env.addresses().of(node, address).maxDeliveryAttempts();
        if (limit.isPresent() && limit.getAsInt() != -1 && retry.maxAttempts() > limit.getAsInt()) {
            Remedy remedy = ArtemisRemedies.maxDeliveryAttempts(address, retry.maxAttempts());
            throw new BrokerException(
                    Status.FAILED_PRECONDITION,
                    ErrorInfo.of(ErrorReasons.PRECONDITION_FAILED),
                    Dispatch.NOT_SENT,
                    node.id(),
                    "The retry policy allows " + retry.maxAttempts()
                            + " attempts, but the broker dead-letters a message of "
                            + address + " after " + limit.getAsInt() + " deliveries. " + remedy.text()
                            + " In broker.xml:\n" + remedy.snippet());
        }
    }

    @FunctionalInterface
    private interface Open<T> {
        T open() throws ActiveMQException;
    }

    private <T> T opened(NodeHandle node, String operation, ResourceId target, Open<T> open) {
        try (Permit permit = env.links().admit(node, operation, Optional.of(target))) {
            try {
                T opened = open.open();
                permit.complete(Outcome.succeeded(), Optional.empty());
                return opened;
            } catch (ActiveMQException e) {
                BrokerException failure = CoreLinks.failure(node.id(), e, Optional.of(target));
                permit.complete(Outcome.failed(failure), Optional.empty());
                throw failure;
            } catch (BrokerException e) {
                permit.complete(Outcome.failed(e), Optional.empty());
                throw e;
            }
        }
    }

    /** Closes every link this transport opened. */
    @Override
    public void close() {
        env.close();
    }
}
