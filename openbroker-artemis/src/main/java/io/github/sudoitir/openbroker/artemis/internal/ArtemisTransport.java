package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.messaging.DeduplicationWindow;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Enforcement;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageHandler;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PrefetchBound;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.ReceiveMode;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.RetrySupport;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.SettlementScope;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.jspecify.annotations.Nullable;

/**
 * Receivers and senders over Core sessions of their own. Opening one and committing are admitted by
 * the caller's interceptor; receiving and sending each message are not.
 *
 * <p>Settling on Artemis: {@code Accept} acknowledges the message alone. {@code Release} without a
 * failed delivery hands back every message the receiver holds unsettled, uncounted; with a failed
 * delivery, or {@code Reject}, they are handed back counted, so the broker's max-delivery-attempts
 * dead-letters a message rejected often enough. A delay is not supported.
 */
public final class ArtemisTransport implements Transport {

    /** How long a receive waits for each message after the first. */
    private static final Duration NEXT = Duration.ofMillis(200);

    private final Cluster cluster;
    private final CoreLinks links;

    ArtemisTransport(Cluster cluster, CoreLinks links) {
        this.cluster = cluster;
        this.links = links;
    }

    private static final TransportCapabilities CAPABILITIES = new TransportCapabilities(
            Set.of(Guarantee.AT_LEAST_ONCE, Guarantee.TRANSACTED),
            false,
            Set.of(OrderingScope.NONE),
            Set.of(PrefetchBound.BYTES),
            SettlementScope.RECEIVER,
            false,
            new RetrySupport(Enforcement.SYSTEM, Enforcement.SYSTEM, Enforcement.SYSTEM),
            false);

    @Override
    public TransportCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public Processor process(ResourceId subscription, Scope scope, ProcessorOptions options, MessageHandler handler) {
        throw Unimplemented.of("process messages yet");
    }

    @Override
    public AutoCloseable watch(LinkListener listener) {
        throw Unimplemented.of("report link changes yet");
    }

    private static boolean transacted(Guarantee guarantee) {
        if (!CAPABILITIES.guarantees().contains(guarantee)) {
            throw Unimplemented.of("open a link with the guarantee " + guarantee);
        }
        return guarantee == Guarantee.TRANSACTED;
    }

    @Override
    public Receiver receiver(ResourceId subscription, Scope scope, ReceiverOptions options) {
        requireQueue(subscription);
        List<NodeHandle> nodes = cluster.nodes(scope);
        if (nodes.size() != 1) {
            throw Queries.invalid("A receiver reads one node; scope it to one node instance.");
        }
        NodeHandle node = nodes.getFirst();
        Deadline deadline = Deadlines.current(cluster.options());
        String selector = Selectors.bounded(Selectors.of(options.filter()), options.producedAtOrBefore());
        String destination = Destinations.of(cluster, node, subscription, deadline);
        boolean browse = options.mode() == ReceiveMode.BROWSE;
        boolean transacted = transacted(options.guarantee());
        boolean buffered =
                options.flow().prefetchMessages() > 0 || options.flow().prefetchBytes() > 0;
        return opened(node, "receiver.open", subscription, () -> {
            CoreLinks.Lease lease = links.lease(
                    node,
                    new CoreLinks.LinkSettings(
                            CoreLinks.Purpose.TRANSPORT, buffered ? CoreLinks.WINDOW_BYTES : 0, -1, true, true, false),
                    transacted,
                    true);
            ClientSession session = lease.session();
            try {
                ClientConsumer consumer = session.createConsumer(
                        SimpleString.of(destination), selector.isEmpty() ? null : SimpleString.of(selector), browse);
                return new CoreReceiver(node, subscription, lease, consumer, transacted, browse);
            } catch (ActiveMQException | RuntimeException e) {
                lease.close();
                throw e;
            }
        });
    }

    @Override
    public Sender sender(SendTarget target, Scope scope, SenderOptions options) {
        boolean transacted = transacted(options.guarantee());
        Deadline deadline = Deadlines.current(cluster.options());
        BrokerException last = null;
        for (NodeHandle node : cluster.nodes(scope)) {
            try {
                ResourceId resource;
                String destination;
                switch (target) {
                    case SendTarget.ToSubscription s -> {
                        requireQueue(s.subscription());
                        resource = s.subscription();
                        destination = Destinations.of(cluster, node, resource, deadline);
                    }
                    case SendTarget.ToDestination d -> {
                        if (!d.destination().kind().equals(ArtemisKinds.ADDRESS)
                                || d.partition().isPresent()
                                || d.routingKey().isPresent()) {
                            throw Queries.invalid("An Artemis destination is an address, with no partition or key.");
                        }
                        resource = d.destination();
                        destination = resource.name();
                    }
                }
                return opened(node, "sender.open", resource, () -> {
                    CoreLinks.Lease lease = links.lease(
                            node,
                            new CoreLinks.LinkSettings(
                                    CoreLinks.Purpose.TRANSPORT, CoreLinks.WINDOW_BYTES, -1, true, true, false),
                            transacted,
                            true);
                    try {
                        return new CoreSender(
                                node, resource, lease, lease.session().createProducer(), destination, transacted);
                    } catch (ActiveMQException | RuntimeException e) {
                        lease.close();
                        throw e;
                    }
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

    @FunctionalInterface
    private interface Open<T> {
        T open() throws ActiveMQException;
    }

    private <T> T opened(NodeHandle node, String operation, ResourceId target, Open<T> open) {
        try (Permit permit = links.admit(node, operation, Optional.of(target))) {
            try {
                T opened = open.open();
                permit.complete(Outcome.succeeded(), Optional.empty());
                return opened;
            } catch (ActiveMQException e) {
                BrokerException failure = CoreLinks.failure(node.id(), e);
                permit.complete(Outcome.failed(failure), Optional.empty());
                throw failure;
            } catch (BrokerException e) {
                permit.complete(Outcome.failed(e), Optional.empty());
                throw e;
            }
        }
    }

    private static void requireQueue(ResourceId id) {
        if (!id.kind().equals(ArtemisKinds.QUEUE)) {
            throw Queries.invalid(
                    "Artemis holds messages in a queue, " + ArtemisKinds.QUEUE + "; not " + id.kind() + ".");
        }
    }

    /** Owner-only spool files for one receiver or sender, removed with it. */
    private final class SpoolDirectory implements CoreBodies.Spool, AutoCloseable {

        private final Path dir;

        SpoolDirectory() {
            this.dir = cluster.options().spoolDirectory().resolve("openbroker-" + UUID.randomUUID());
        }

        @Override
        public Path file(long size) throws IOException {
            boolean posix =
                    FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
            if (!Files.exists(dir)) {
                if (posix) {
                    Files.createDirectories(
                            dir, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
                } else {
                    Files.createDirectories(dir);
                }
            }
            long usable = Files.getFileStore(dir).getUsableSpace();
            if (usable < size) {
                throw new IOException("A large message of " + size + " bytes needs that much free disk under " + dir
                        + ", and only " + usable + " bytes are free.");
            }
            FileAttribute<?>[] attributes = posix
                    ? new FileAttribute<?>[] {
                        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))
                    }
                    : new FileAttribute<?>[0];
            return Files.createTempFile(dir, "message-", ".body", attributes);
        }

        void delete(@Nullable Path file) {
            if (file != null) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    throw new UncheckedIOException("Could not delete the spool file " + file, e);
                }
            }
        }

        @Override
        public void close() {
            if (!Files.exists(dir)) {
                return;
            }
            try (Stream<Path> paths = Files.walk(dir)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        // One undeletable file must not keep the rest.
                    }
                });
            } catch (IOException e) {
                throw new UncheckedIOException("Could not remove the spool directory " + dir, e);
            }
        }
    }

    private final class CoreReceiver implements Receiver {

        private final NodeHandle node;
        private final ResourceId subscription;
        private final CoreLinks.Lease lease;
        private final ClientSession session;
        private final ClientConsumer consumer;
        private final boolean transacted;
        private final boolean browse;
        private final SpoolDirectory spool = new SpoolDirectory();
        private final Map<String, Held> unsettled = new LinkedHashMap<>();
        private final String id = UUID.randomUUID().toString();
        private volatile boolean closed;

        private record Held(ClientMessage message, @Nullable Path file) {}

        CoreReceiver(
                NodeHandle node,
                ResourceId subscription,
                CoreLinks.Lease lease,
                ClientConsumer consumer,
                boolean transacted,
                boolean browse) {
            this.node = node;
            this.subscription = subscription;
            this.lease = lease;
            this.session = lease.session();
            this.consumer = consumer;
            this.transacted = transacted;
            this.browse = browse;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public LinkState state() {
            return closed ? LinkState.CLOSED : LinkState.OPEN;
        }

        @Override
        public Sender sender(SendTarget target) {
            throw Unimplemented.of("join a sender to a receiver's transaction yet");
        }

        @Override
        public List<Message> receive(int max, Duration timeout) {
            List<Message> out = new ArrayList<>();
            try {
                ClientMessage next = consumer.receive(Math.max(1, timeout.toMillis()));
                while (next != null) {
                    out.add(hold(next));
                    if (out.size() >= max) {
                        break;
                    }
                    next = consumer.receive(NEXT.toMillis());
                }
                return out;
            } catch (ActiveMQException e) {
                throw CoreLinks.failure(node.id(), e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private Message hold(ClientMessage m) throws ActiveMQException, IOException {
            Path[] file = new Path[1];
            CoreBodies.Spool tracking = size -> file[0] = spool.file(size);
            Message message = CoreBodies.read(m, subscription, node.id(), Long.MAX_VALUE, Optional.of(tracking));
            unsettled.put(message.ref().position(), new Held(m, file[0]));
            return message;
        }

        @Override
        public void settle(Message message, Disposition disposition) {
            Held held = unsettled.remove(message.ref().position());
            if (held == null) {
                throw Queries.invalid("This receiver holds no unsettled message "
                        + message.ref().position() + ".");
            }
            spool.delete(held.file());
            if (browse) {
                return;
            }
            try {
                switch (disposition) {
                    case Disposition.Accept a -> held.message().individualAcknowledge();
                    case Disposition.Release r
                    when r.delay().isPresent() -> throw Unimplemented.of("release an Artemis message after a delay");
                    case Disposition.Release r -> {
                        requireNotTransacted();
                        releaseAll(r.deliveryFailed());
                    }
                    case Disposition.Reject r -> {
                        requireNotTransacted();
                        releaseAll(true);
                    }
                }
            } catch (ActiveMQException e) {
                throw CoreLinks.failure(node.id(), e);
            }
        }

        private void requireNotTransacted() {
            if (transacted) {
                throw Queries.invalid("A transacted receiver settles its batch with commit or rollback.");
            }
        }

        /** Hands back every unsettled message; counted, they move toward dead-lettering. */
        private void releaseAll(boolean counted) throws ActiveMQException {
            session.rollback(counted);
            unsettled.values().forEach(h -> spool.delete(h.file()));
            unsettled.clear();
        }

        @Override
        public void commit() {
            if (!transacted) {
                throw Queries.invalid("This receiver settles each message; it has no batch to commit.");
            }
            try (Permit permit = links.admit(node, "receiver.commit", Optional.of(subscription))) {
                try {
                    session.commit();
                    permit.complete(Outcome.succeeded(), Optional.empty());
                } catch (ActiveMQException e) {
                    BrokerException failure = CoreLinks.failure(node.id(), e);
                    permit.complete(Outcome.failed(failure), Optional.empty());
                    throw failure;
                }
            }
            unsettled.values().forEach(h -> spool.delete(h.file()));
            unsettled.clear();
        }

        @Override
        public void rollback() {
            try {
                session.rollback();
            } catch (ActiveMQException e) {
                throw CoreLinks.failure(node.id(), e);
            } finally {
                unsettled.values().forEach(h -> spool.delete(h.file()));
                unsettled.clear();
            }
        }

        @Override
        public void close() {
            closed = true;
            try {
                consumer.close();
            } catch (ActiveMQException e) {
                // teardown
            }
            // The broker hands back whatever this session holds unsettled.
            lease.close();
            unsettled.clear();
            spool.close();
        }
    }

    private final class CoreSender implements Sender {

        private final NodeHandle node;
        private final ResourceId target;
        private final CoreLinks.Lease lease;
        private final ClientSession session;
        private final ClientProducer producer;
        private final SimpleString destination;
        private final boolean transacted;
        private final SpoolDirectory spool = new SpoolDirectory();
        private final List<CoreBodies.Built> pending = new ArrayList<>();
        private final String id = UUID.randomUUID().toString();
        private volatile boolean closed;

        CoreSender(
                NodeHandle node,
                ResourceId target,
                CoreLinks.Lease lease,
                ClientProducer producer,
                String destination,
                boolean transacted) {
            this.node = node;
            this.target = target;
            this.lease = lease;
            this.session = lease.session();
            this.producer = producer;
            this.destination = SimpleString.of(destination);
            this.transacted = transacted;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public LinkState state() {
            return closed ? LinkState.CLOSED : LinkState.OPEN;
        }

        @Override
        public Optional<DeduplicationWindow> deduplication() {
            return Optional.empty();
        }

        @Override
        public Offer offer(OutboundMessage message) {
            throw Unimplemented.of("offer a message without blocking yet");
        }

        @Override
        public void flush(Duration timeout) {
            throw Unimplemented.of("flush a sender yet");
        }

        @Override
        public SendReceipt send(OutboundMessage message) {
            try {
                CoreBodies.Built built =
                        CoreBodies.write(session, message, cluster.options().clock(), Optional.of(spool));
                producer.send(destination, built.message());
                if (transacted) {
                    pending.add(built);
                } else {
                    built.close();
                }
                Object id = built.message().getUserID();
                return new SendReceipt(
                        id == null ? Optional.empty() : Optional.of("ID:" + id),
                        Optional.empty(),
                        Optional.of(node.id()),
                        false);
            } catch (ActiveMQException e) {
                throw CoreLinks.failure(node.id(), e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void commit() {
            if (!transacted) {
                throw Queries.invalid("This sender delivers each message as it is sent; it has no batch to commit.");
            }
            try (Permit permit = links.admit(node, "sender.commit", Optional.of(target))) {
                try {
                    session.commit();
                    permit.complete(Outcome.succeeded(), Optional.empty());
                } catch (ActiveMQException e) {
                    BrokerException failure = CoreLinks.failure(node.id(), e);
                    permit.complete(Outcome.failed(failure), Optional.empty());
                    throw failure;
                }
            } finally {
                release();
            }
        }

        @Override
        public void rollback() {
            try {
                session.rollback();
            } catch (ActiveMQException e) {
                throw CoreLinks.failure(node.id(), e);
            } finally {
                release();
            }
        }

        private void release() {
            for (CoreBodies.Built built : pending) {
                try {
                    built.close();
                } catch (IOException e) {
                    // the spool directory goes with the sender
                }
            }
            pending.clear();
        }

        @Override
        public void close() {
            closed = true;
            release();
            try {
                producer.close();
            } catch (ActiveMQException e) {
                // teardown
            }
            lease.close();
            spool.close();
        }
    }
}
