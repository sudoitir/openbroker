package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.codec.RecordCodec;
import io.github.sudoitir.openbroker.messaging.DeduplicationWindow;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.support.messaging.LinkSupervisor;
import io.github.sudoitir.openbroker.support.messaging.SendPipeline;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.jspecify.annotations.Nullable;

/**
 * A sender over a {@link SendPipeline}: the pipeline bounds what is unconfirmed and writes it again
 * after a recovery; this class is its writer on a Kafka producer.
 *
 * <ul>
 *   <li>{@code AT_LEAST_ONCE}: {@code acks=all} with idempotence, so the client's own retries are
 *       invisible and deduplicated. A producer that dies (fenced, out of sequence, timed out
 *       delivering) is replaced by the supervisor, and the messages it had not confirmed are written
 *       again; only those receipts say {@code resent}.
 *   <li>{@code AT_MOST_ONCE}: {@code acks=1}; a message that is not confirmed is never written again.
 *   <li>{@code TRANSACTED}: sends join the producer's transaction and {@link #commit} ends it. A
 *       sender joined to a receiver or a processor lane is ended by its owner, never by itself.
 * </ul>
 *
 * Deduplication has the {@code SESSION} scope: no window is reported. Messages are durable; a message
 * that says it is not is refused under {@code AT_LEAST_ONCE} and {@code TRANSACTED}.
 */
final class KafkaSender implements Sender {

    /** Where a sender writes. */
    record Target(String topic, @Nullable Integer partition, Optional<String> routingKey, ResourceId resource) {}

    private final MessagingEnv env;
    private final Target target;
    private final SenderOptions options;
    private final String id = UUID.randomUUID().toString();
    private final SendPipeline pipeline;
    private final @Nullable LinkSupervisor supervisor;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile Backend backend;
    private int generation;
    private int batchGeneration = -1;
    private boolean closed;

    /** The producer a sender writes through, and the transaction when it has one. */
    private record Backend(
            KafkaProducer<byte[], byte[]> producer,
            @Nullable Transaction transaction) {}

    /** A sender that owns its producer. */
    static KafkaSender open(MessagingEnv env, Target target, SenderOptions options) {
        return new KafkaSender(env, target, options, null);
    }

    /** A sender joined to the transaction of its owner, which ends it. */
    static KafkaSender joined(MessagingEnv env, Target target, SenderOptions options, Transaction transaction) {
        return new KafkaSender(env, target, options, transaction);
    }

    private KafkaSender(MessagingEnv env, Target target, SenderOptions options, @Nullable Transaction joined) {
        this.env = env;
        this.target = target;
        this.options = options;
        KafkaCluster cluster = env.cluster();
        this.backend = joined != null ? new Backend(joined.producer(), joined) : create();
        this.pipeline = SendPipeline.open(
                id,
                KafkaCluster.CLUSTER,
                options,
                env.info(OperationType.SEND, target.resource(), Optional.empty(), 1),
                env.engine(),
                this::write,
                cause -> {
                    // The write that found the link down has already started its recovery.
                });
        if (joined != null) {
            this.supervisor = null;
        } else {
            this.supervisor = new LinkSupervisor(
                    id,
                    KafkaCluster.CLUSTER,
                    cluster.bootstrapServers(),
                    options.recovery(),
                    new Recover(),
                    env.engine());
            this.supervisor.listen(env::fire);
        }
    }

    private Backend create() {
        if (options.guarantee() == Guarantee.TRANSACTED) {
            Transaction transaction = new Transaction(env.cluster(), "openbroker-sender-" + id + "-" + generation);
            return new Backend(transaction.producer(), transaction);
        }
        return new Backend(Producers.create(env.cluster(), options.guarantee(), null), null);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public LinkState state() {
        if (closed) {
            return LinkState.CLOSED;
        }
        return supervisor == null ? LinkState.OPEN : supervisor.state();
    }

    @Override
    public Optional<DeduplicationWindow> deduplication() {
        return Optional.empty();
    }

    @Override
    public SendReceipt send(OutboundMessage message) {
        requireDurable(message);
        return pipeline.send(message);
    }

    @Override
    public Offer offer(OutboundMessage message) {
        requireDurable(message);
        return pipeline.offer(message);
    }

    @Override
    public void flush(Duration timeout) {
        pipeline.flush(timeout);
    }

    private void requireDurable(OutboundMessage message) {
        if (options.guarantee() != Guarantee.AT_MOST_ONCE
                && message.headers().durable().filter(durable -> !durable).isPresent()) {
            throw KafkaFailures.invalid("A message sent " + options.guarantee()
                    + " is durable; a non-durable one is refused. Leave durable unset, or send AT_MOST_ONCE.");
        }
    }

    // ---- the wire ---------------------------------------------------------------------------

    private void write(OutboundMessage message, SendPipeline.Confirmation confirmation) {
        Backend current = backend;
        int written = generation;
        ProducerRecord<byte[], byte[]> record;
        try {
            record = RecordCodec.record(
                    target.topic(), Optional.ofNullable(target.partition()), target.routingKey(), message);
        } catch (RuntimeException e) {
            throw KafkaFailures.of(e, Optional.of(target.resource()));
        }
        try {
            if (current.transaction() != null) {
                current.transaction().ensureBegun();
                markBatch(written);
            }
            current.producer().send(record, (metadata, failure) -> {
                if (failure == null) {
                    confirmation.confirmed(receipt(metadata));
                } else {
                    answered(failure, written, confirmation);
                }
            });
        } catch (RuntimeException e) {
            throw refused(e, written);
        }
    }

    /** The answer of a failed send: a lost producer is recovered and the message written again, any other failure ends this send. */
    private void answered(Throwable failure, int written, SendPipeline.Confirmation confirmation) {
        if (supervisor != null && producerLost(failure) && options.guarantee() != Guarantee.AT_MOST_ONCE) {
            lost(written, failure);
            return;
        }
        if (supervisor == null && producerLost(failure)) {
            markOwnersProducerBroken();
        }
        BrokerException mapped = KafkaFailures.of(failure, Optional.of(target.resource()));
        confirmation.failed(
                options.guarantee() == Guarantee.AT_MOST_ONCE && producerLost(failure) ? inDoubt(mapped) : mapped);
    }

    /** The failure of a write that threw: a dead producer keeps the message for the recovery; a refusal ends its receipt. */
    private BrokerException refused(RuntimeException e, int written) {
        // Joined to its owner's producer there is no recovery to wait for: the owner replaces the producer.
        if (supervisor == null && producerLost(e)) {
            markOwnersProducerBroken();
        }
        if (supervisor != null && producerLost(e)) {
            BrokerException down = down(e);
            lost(written, e);
            return down;
        }
        return KafkaFailures.of(e, Optional.of(target.resource()));
    }

    private void markOwnersProducerBroken() {
        Transaction owner = backend.transaction();
        if (owner != null) {
            owner.markBroken();
        }
    }

    private void lost(int written, Throwable cause) {
        if (supervisor != null && written == generation) {
            supervisor.lost(down(cause));
        }
    }

    private static BrokerException down(Throwable cause) {
        return new BrokerException(
                Status.UNAVAILABLE,
                ErrorInfo.of(ErrorReasons.UNREACHABLE),
                Dispatch.NOT_SENT,
                KafkaCluster.CLUSTER,
                "The producer was lost: " + cause.getMessage(),
                cause);
    }

    private static BrokerException inDoubt(BrokerException failure) {
        return new BrokerException(
                failure.status(),
                failure.info(),
                Dispatch.IN_DOUBT,
                KafkaCluster.CLUSTER,
                failure.getMessage(),
                failure);
    }

    /** Whether the producer itself is gone, as opposed to the cluster refusing one record. */
    static boolean producerLost(Throwable failure) {
        return failure instanceof ProducerFencedException
                || failure instanceof InvalidProducerEpochException
                || failure instanceof OutOfOrderSequenceException
                || failure instanceof UnknownProducerIdException
                || failure instanceof RetriableException
                || !(failure instanceof ApiException);
    }

    private SendReceipt receipt(RecordMetadata metadata) {
        ResourceId resource = target.resource();
        return new SendReceipt(
                Optional.of(metadata.topic() + "-" + metadata.partition() + "@" + metadata.offset()),
                Optional.of(new MessageRef(
                        resource,
                        Shard.partition(PartitionId.of(Integer.toString(metadata.partition()))),
                        Long.toString(metadata.offset()))),
                Optional.of(KafkaCluster.CLUSTER),
                false);
    }

    // ---- recovery ---------------------------------------------------------------------------

    private final class Recover implements LinkSupervisor.Link {

        @Override
        public void suspended(BrokerException cause) {
            pipeline.suspend(cause);
        }

        @Override
        public NodeId reopen() {
            Backend old = backend;
            lock.lock();
            try {
                generation++;
                backend = create();
            } finally {
                lock.unlock();
            }
            closeQuietly(old);
            pipeline.resume(KafkaSender.this::write);
            return KafkaCluster.CLUSTER;
        }

        @Override
        public void failed(BrokerException cause) {
            pipeline.fail(cause);
        }
    }

    // ---- the transaction ----------------------------------------------------------------------

    private void markBatch(int written) {
        lock.lock();
        try {
            if (batchGeneration == -1) {
                batchGeneration = written;
            } else if (batchGeneration != written) {
                // The producer was replaced inside the transaction: what came before is gone.
                batchGeneration = Integer.MIN_VALUE;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit() {
        Transaction transaction = ownTransaction("commit");
        pipeline.flush(env.cluster().options().timeouts().call());
        try {
            lock.lock();
            try {
                if (batchGeneration != -1 && batchGeneration != generation) {
                    abortQuietly(transaction);
                    throw rolledBack();
                }
            } finally {
                lock.unlock();
            }
            env.admitted("sender.commit", target.resource(), () -> {
                transaction.commit(Map.of(), null);
                return null;
            });
        } catch (RuntimeException e) {
            if (!(e instanceof BrokerException)) {
                abortQuietly(transaction);
            }
            throw e;
        } finally {
            endBatch();
        }
    }

    @Override
    public void rollback() {
        Transaction transaction = ownTransaction("rollback");
        try {
            transaction.abort();
        } catch (RuntimeException e) {
            throw KafkaFailures.of(e, Optional.of(target.resource()));
        } finally {
            endBatch();
        }
    }

    private Transaction ownTransaction(String what) {
        Transaction transaction = backend.transaction();
        if (options.guarantee() != Guarantee.TRANSACTED || transaction == null) {
            throw KafkaFailures.precondition("Only a TRANSACTED sender can " + what + ".");
        }
        if (supervisor == null) {
            throw KafkaFailures.precondition(
                    "This sender joined a transaction its receiver or processor ends: it cannot " + what + ".");
        }
        return transaction;
    }

    private void endBatch() {
        lock.lock();
        try {
            batchGeneration = -1;
        } finally {
            lock.unlock();
        }
    }

    private static BrokerException rolledBack() {
        return new BrokerException(
                Status.ABORTED,
                ErrorInfo.of(ErrorReasons.TRANSACTION_ROLLED_BACK),
                Dispatch.REJECTED,
                KafkaCluster.CLUSTER,
                "The producer was lost and the transaction was rolled back");
    }

    private static void abortQuietly(Transaction transaction) {
        try {
            transaction.abort();
        } catch (RuntimeException e) {
            // a producer that cannot abort is replaced by the recovery
        }
    }

    private static void closeQuietly(Backend backend) {
        try {
            if (backend.transaction() != null) {
                backend.transaction().close();
            } else {
                backend.producer().close(Duration.ofSeconds(2));
            }
        } catch (RuntimeException e) {
            // closing a dead producer may fail
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            pipeline.close();
        } finally {
            closed = true;
            if (supervisor != null) {
                supervisor.close();
                closeQuietly(backend);
            }
            env.forget(this);
        }
    }
}
