package io.github.sudoitir.openbroker.rabbitmq.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.BrowsePage;
import io.github.sudoitir.openbroker.messaging.BrowseRequest;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.messaging.Denominator;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.Total;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import io.github.sudoitir.openbroker.rabbitmq.amqp.MessageCodec;
import io.github.sudoitir.openbroker.rabbitmq.amqp.PooledAmqpConnections;
import io.github.sudoitir.openbroker.rabbitmq.amqp.StreamOffsets;
import io.github.sudoitir.openbroker.support.Deadlines;
import io.github.sudoitir.openbroker.support.PortableFilter;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * The browsing {@link Receiver} of a stream and the engine behind {@code Messages.browse}: starts at an offset, bounded
 * by production time, and ends at the committed offset at open. It acknowledges what it reads, which never removes
 * anything from a stream.
 *
 * <p>A read opens its own channel, consumes with {@code x-stream-offset}, acknowledges each delivery as it is read (on
 * a stream an acknowledgement only returns credit), stops at the page size, at the end or at the deadline, and closes
 * the channel. It never waits for messages written after it began. A time position or a production-time bound has the
 * granularity of an AMQP timestamp (one second) and of a chunk, so a read may begin a little before the time it names.
 */
final class StreamReader implements Receiver {

    /** The most deliveries a read lets the server send ahead of its acknowledgements. */
    static final int MAX_PREFETCH = 500;

    /** How long a read waits for the next delivery before it takes the stream to have no more. */
    static final Duration IDLE = Duration.ofSeconds(1);

    private static final long UNKNOWN_END = Long.MAX_VALUE;
    private static final long NONE = -1;
    private static final Duration HINTED_IDLE = Duration.ofMillis(200);
    private static final Duration TIME_GRAIN = Duration.ofSeconds(1);

    /** The first offset a stream was seen to retain, and when. */
    record Known(long firstOffset, Instant at) {}

    /**
     * What management says of a stream: the committed offset when this RabbitMQ version reports it (4.4 does not),
     * the sampled count of messages, and the time of the first retained one.
     */
    private record Stats(OptionalLong committed, long messages, long firstTimestamp) {
        long end() {
            return committed.orElse(UNKNOWN_END);
        }

        boolean knownEnd() {
            return committed.isPresent();
        }
    }

    private record Event(
            @Nullable Delivery delivery, @Nullable Throwable failure) {}

    private record Page(List<Message> messages, long firstOffset, long lastOffset) {
        static final Page EMPTY = new Page(List.of(), NONE, NONE);
    }

    private final RabbitMqCluster cluster;
    private final ResourceId stream;
    private final String vhost;
    private final String name;
    private final AmqpConnections connections;
    private final boolean ownsConnections;
    private final ReceiverOptions options;
    private final ConcurrentMap<ResourceId, Known> known;
    private final String id = "stream-reader-" + UUID.randomUUID();
    private Object nextArgument = StreamOffsets.FIRST;
    private @Nullable Long stopBefore;
    private boolean closed;

    /** A browsing receiver of {@code stream} over connections of its own. */
    StreamReader(RabbitMqCluster cluster, ResourceId stream) {
        this(
                cluster,
                stream,
                new PooledAmqpConnections(cluster),
                true,
                ReceiverOptions.browse(Optional.empty()),
                new ConcurrentHashMap<>());
    }

    StreamReader(
            RabbitMqCluster cluster,
            ResourceId stream,
            AmqpConnections connections,
            boolean ownsConnections,
            ReceiverOptions options,
            ConcurrentMap<ResourceId, Known> known) {
        requireStream(stream);
        this.cluster = cluster;
        this.stream = stream;
        this.vhost = stream.path().segments().get(0);
        this.name = stream.name();
        this.connections = connections;
        this.ownsConnections = ownsConnections;
        this.options = options;
        this.known = known;
    }

    /**
     * Refuses a resource that is not a stream, before any channel or request.
     *
     * @throws BrokerException {@code UNIMPLEMENTED}/{@code NOT_SUPPORTED} for a queue, {@code INVALID_ARGUMENT} for
     *     anything else
     */
    static void requireStream(ResourceId resource) {
        String kind = resource.kind().value();
        if (RabbitMqKinds.STREAM.value().equals(kind)) {
            if (resource.path().segments().size() != 2) {
                throw RabbitMqErrors.invalid("A stream is named by its virtual host and its name.");
            }
            return;
        }
        if (RabbitMqKinds.CLASSIC_QUEUE.value().equals(kind)
                || RabbitMqKinds.QUORUM_QUEUE.value().equals(kind)) {
            throw refused(
                    "RabbitMQ gives a queue's messages only to a consumer, which takes them off the queue: a queue"
                            + " cannot be browsed. Browse a stream, which keeps its messages.");
        }
        throw RabbitMqErrors.invalid("Messages are in a queue or a stream, not in " + kind + ".");
    }

    /** The failure for what RabbitMQ cannot do: unimplemented, {@code NOT_SUPPORTED}, and not sent. */
    static BrokerException refused(String message) {
        return new BrokerException(
                Status.UNIMPLEMENTED, ErrorInfo.of(ErrorReasons.NOT_SUPPORTED), Dispatch.NOT_SENT, null, message);
    }

    /** Reads one page of the stream as {@code request} asks. */
    BrowsePage browse(BrowseRequest request) {
        Deadline deadline = Deadlines.current(cluster.options());
        Optional<PortableFilter> filter = portable(request.filter());
        Object argument = StreamOffsets.argument(request.position());
        return cluster.call(
                "messages.browse", Optional.of(stream), deadline, () -> page(request, argument, filter, deadline));
    }

    private BrowsePage page(
            BrowseRequest request, Object argument, Optional<PortableFilter> filter, Deadline deadline) {
        Stats stats = stats(deadline);
        if (request.position() instanceof Position.After after) {
            long issued = StreamOffsets.cursorFirstTimestamp(after.cursor()).toEpochMilli();
            // Management reports 0 until the stream's statistics are first emitted: then nothing can be compared.
            if (issued != 0 && stats.firstTimestamp() != 0 && issued != stats.firstTimestamp()) {
                throw new BrokerException(
                        Status.FAILED_PRECONDITION,
                        ErrorInfo.of(ErrorReasons.PRECONDITION_FAILED),
                        Dispatch.NOT_SENT,
                        null,
                        "The stream was recreated or truncated past the cursor: browse it again from the start.");
            }
        }
        long end = stats.end();
        boolean empty = nothingToRead(argument, stats);
        if (argument instanceof Long start && stats.knownEnd() && start > end + 1) {
            throw outOfRange(start, stats);
        }
        Page read = empty
                ? Page.EMPTY
                : read(argument, request.limit(), filter, request.maxBodyBytes(), end, hint(stats), deadline);
        if (argument instanceof Long start && read.firstOffset() != NONE && read.firstOffset() != start) {
            throw outOfRange(start, stats);
        }
        Instant now = cluster.options().clock().instant();
        if (StreamOffsets.FIRST.equals(argument) && read.firstOffset() != NONE) {
            known.put(stream, new Known(read.firstOffset(), now));
        }
        long hint = hint(stats);
        boolean more = read.lastOffset() != NONE
                && (stats.knownEnd()
                        ? read.lastOffset() < end
                        : read.messages().size() >= request.limit()
                                || (hint != UNKNOWN_END && read.lastOffset() < hint));
        Optional<Cursor> next = more
                ? Optional.of(StreamOffsets.cursor(read.lastOffset(), Instant.ofEpochMilli(stats.firstTimestamp())))
                : Optional.empty();
        return new BrowsePage(read.messages(), next, total(stats));
    }

    private static boolean nothingToRead(Object argument, Stats stats) {
        if (!stats.knownEnd()) {
            return false;
        }
        if (StreamOffsets.NEXT.equals(argument)) {
            return true;
        }
        return argument instanceof Long start && start > stats.end();
    }

    private BrokerException outOfRange(long start, Stats stats) {
        return new BrokerException(
                Status.OUT_OF_RANGE,
                ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT),
                Dispatch.NOT_SENT,
                null,
                "Offset " + start + " is outside " + stream.name() + ", which holds offsets up to "
                        + (stats.knownEnd() ? Long.toString(stats.end()) : "an unknown end")
                        + " and has dropped what came before its first retained one.");
    }

    /**
     * The estimate of the messages the stream holds: management's sampled count, or, while management has not yet
     * sampled the stream (it reports 0 for a few seconds after a write), the span from the first to the last offset.
     */
    Total total(Deadline deadline) {
        Stats stats = stats(deadline);
        if (stats.messages() > 0) {
            return total(stats);
        }
        long first = read(StreamOffsets.FIRST, 1, Optional.empty(), 0, UNKNOWN_END, UNKNOWN_END, deadline)
                .firstOffset();
        if (first == NONE) {
            return new Total(Denominator.ALL, Count.exact(0));
        }
        long last = read(StreamOffsets.LAST, MAX_PREFETCH, Optional.empty(), 0, UNKNOWN_END, UNKNOWN_END, deadline)
                .lastOffset();
        return new Total(Denominator.ALL, Count.estimate(Math.max(1, last - first + 1)));
    }

    private static Total total(Stats stats) {
        return new Total(Denominator.ALL, Count.estimate(stats.messages()));
    }

    /**
     * Where the stream probably ends, from its sampled count and its first retained offset when a browse learned it
     * lately; unknown otherwise. It is a hint: the count lags, so a read that reaches it waits a little for more.
     */
    private long hint(Stats stats) {
        Known seen = known.get(stream);
        if (stats.knownEnd() || stats.messages() <= 0 || seen == null) {
            return UNKNOWN_END;
        }
        Duration age = Duration.between(seen.at(), cluster.options().clock().instant());
        if (age.compareTo(RabbitMqOptions.watchInterval(cluster.spec())) >= 0) {
            return UNKNOWN_END;
        }
        return seen.firstOffset() + stats.messages() - 1;
    }

    private Stats stats(Deadline deadline) {
        JsonNode info = RabbitMqMessages.queueInfo(cluster, vhost, name, deadline, "messages.stream-info");
        JsonNode committed = info.path("committed_offset");
        return new Stats(
                committed.isNumber() ? OptionalLong.of(committed.asLong()) : OptionalLong.empty(),
                info.path("messages").asLong(0),
                info.path("first_timestamp").asLong(0));
    }

    private static Optional<PortableFilter> portable(Optional<MessageFilter> filter) {
        return filter.map(f -> {
            if (!MessageFilter.PORTABLE.equals(f.dialect())) {
                throw RabbitMqErrors.invalid("A RabbitMQ stream has no filter dialect '" + f.dialect()
                        + "': filter with '" + MessageFilter.PORTABLE + "'.");
            }
            return PortableFilter.parse(f);
        });
    }

    /**
     * Reads up to {@code limit} messages from {@code argument} to offset {@code end} (inclusive). A message the
     * filter drops is read and acknowledged, and moves the position on.
     */
    private Page read(
            Object argument,
            int limit,
            Optional<PortableFilter> filter,
            long maxBodyBytes,
            long end,
            long hint,
            Deadline deadline) {
        Clock clock = cluster.options().clock();
        Connection connection = connections.connection(vhost, deadline);
        Channel channel;
        try {
            channel = connection.createChannel();
        } catch (IOException e) {
            throw RabbitMqErrors.fromIo(e, false);
        }
        BlockingQueue<Event> events = new LinkedBlockingQueue<>();
        List<Message> messages = new ArrayList<>();
        long first = NONE;
        long last = NONE;
        try {
            channel.basicQos(Math.min(limit, MAX_PREFETCH));
            String tag = channel.basicConsume(
                    name,
                    false,
                    Map.of(StreamOffsets.HEADER, argument),
                    (consumerTag, delivery) -> events.add(new Event(delivery, null)),
                    consumerTag -> events.add(new Event(
                            null, new IOException("RabbitMQ cancelled the consumer of the stream " + name + "."))),
                    (consumerTag, signal) -> events.add(new Event(null, signal)));
            while (messages.size() < limit) {
                long idle = (last != NONE && last >= hint ? HINTED_IDLE : IDLE).toMillis();
                long wait = Math.min(deadline.remaining(clock).toMillis(), idle);
                Event event = wait <= 0 ? null : events.poll(wait, TimeUnit.MILLISECONDS);
                if (event == null) {
                    break;
                }
                Throwable failure = event.failure();
                if (failure != null) {
                    throw RabbitMqErrors.fromIo(failure, false);
                }
                Delivery delivery = event.delivery();
                Map<String, Object> headers = delivery.getProperties().getHeaders() == null
                        ? Map.of()
                        : delivery.getProperties().getHeaders();
                long offset = StreamOffsets.offset(headers);
                if (offset > end) {
                    break;
                }
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                first = first == NONE ? offset : first;
                last = offset;
                Message message = MessageCodec.message(
                        stream, delivery.getEnvelope(), delivery.getProperties(), delivery.getBody(), maxBodyBytes);
                if (filter.map(f -> f.test(message)).orElse(true)) {
                    messages.add(message);
                }
                if (offset >= end) {
                    break;
                }
            }
            cancelQuietly(channel, tag);
        } catch (IOException e) {
            throw RabbitMqErrors.fromIo(e, false);
        } catch (InterruptedException e) {
            throw Deadlines.cancelled(RabbitMqCluster.CLUSTER, e);
        } finally {
            closeQuietly(channel);
        }
        return new Page(messages, first, last);
    }

    private static void cancelQuietly(Channel channel, String tag) {
        try {
            if (channel.isOpen()) {
                channel.basicCancel(tag);
            }
        } catch (IOException | ShutdownSignalException e) {
            // The channel is closed next either way.
        }
    }

    private static void closeQuietly(Channel channel) {
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException | ShutdownSignalException e) {
            // The channel is gone either way.
        }
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public LinkState state() {
        return closed ? LinkState.CLOSED : LinkState.OPEN;
    }

    /**
     * Reads up to {@code max} of the messages after the last one read, to the end of the stream as it was when this
     * call began, or to the production-time bound of the receiver's options. It does not wait for new messages, so
     * {@code timeout} only bounds the work.
     */
    @Override
    public List<Message> receive(int max, Duration timeout) {
        if (closed) {
            throw RabbitMqErrors.invalid("The receiver is closed.");
        }
        if (max <= 0) {
            throw RabbitMqErrors.invalid("max must be positive.");
        }
        Deadline deadline = Deadline.after(timeout, cluster.options().clock());
        Optional<PortableFilter> filter = portable(options.filter());
        return cluster.call("messages.receive", Optional.of(stream), deadline, () -> {
            Stats stats = stats(deadline);
            long end = stats.end();
            if (options.producedAtOrBefore().isPresent()) {
                if (stopBefore == null) {
                    stopBefore = firstAfter(options.producedAtOrBefore().get(), deadline);
                }
                end = Math.min(end, stopBefore - 1);
            }
            if (nextArgument instanceof Long start && start > end) {
                return List.<Message>of();
            }
            Page read = read(nextArgument, max, filter, Long.MAX_VALUE, end, hint(stats), deadline);
            if (read.lastOffset() != NONE) {
                nextArgument = read.lastOffset() + 1;
            }
            return read.messages();
        });
    }

    /** The first offset written after {@code bound}, or past the end when nothing was. */
    private long firstAfter(Instant bound, Deadline deadline) {
        Page probe =
                read(Date.from(bound.plus(TIME_GRAIN)), 1, Optional.empty(), 0, UNKNOWN_END, UNKNOWN_END, deadline);
        return probe.firstOffset() == NONE ? UNKNOWN_END : probe.firstOffset();
    }

    /** A browse holds nothing: each delivery was acknowledged as it was read, and a stream keeps its messages. */
    @Override
    public void settle(Message message, Disposition disposition) {
        // Nothing to settle: the acknowledgement happened as the message was read.
    }

    @Override
    public Sender sender(SendTarget target) {
        throw refused("A browsing receiver on a stream cannot send: open a sender for the stream.");
    }

    @Override
    public void commit() {
        throw refused("A browsing receiver is not transacted: RabbitMQ's AMQP transactions are not offered.");
    }

    @Override
    public void rollback() {
        throw refused("A browsing receiver is not transacted: RabbitMQ's AMQP transactions are not offered.");
    }

    @Override
    public void close() {
        closed = true;
        if (ownsConnections) {
            connections.close();
        }
    }
}
