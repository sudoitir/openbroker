package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.codec.RecordCodec;
import io.github.sudoitir.openbroker.messaging.BrowsePage;
import io.github.sudoitir.openbroker.messaging.BrowseRequest;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.messaging.Denominator;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.messaging.Total;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Reads a topic's records without joining a group or committing anything: a consumer that is
 * assigned partitions, never subscribes, and has no group id, so no group appears and none moves. It
 * reads what a read-committed consumer reads: committed records only, and nothing past an open
 * transaction. A page is bounded by the count asked for and by {@link #MAX_PAGE_BYTES}.
 */
final class Browsing {

    /** The most a page holds, whatever the count asked for. */
    static final long MAX_PAGE_BYTES = 16L * 1024 * 1024;

    private static final Duration POLL = Duration.ofMillis(500);

    private Browsing() {}

    static BrowsePage browse(
            Context context, ResourceId source, String topic, Scope scope, BrowseRequest request, Deadline deadline) {
        if (request.filter().isPresent()) {
            throw KafkaErrors.refused(
                    Status.UNIMPLEMENTED,
                    ErrorReasons.NOT_SUPPORTED,
                    "Kafka cannot filter records where they are kept: browse a range of offsets or times.");
        }
        List<TopicPartition> all = Offsets.partitions(context, topic);
        Set<TopicPartition> chosen = Seeks.partitionsIn(scope, all);
        Map<TopicPartition, Long> end = Offsets.latest(context, chosen);
        Map<TopicPartition, Long> start = start(context, chosen, end, request.position());
        Clock clock = context.cluster().options().clock();
        Map<String, Object> config = new HashMap<>(context.cluster().clientConfig(Plane.DATA));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, config.get(ConsumerConfig.CLIENT_ID_CONFIG) + "-browse");
        List<Message> messages = new ArrayList<>();
        Map<TopicPartition, Long> next =
                new TreeMap<>(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
        long bytes = 0;
        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            Map<TopicPartition, Long> readable = readableEnd(consumer, chosen);
            for (TopicPartition tp : chosen.stream()
                    .sorted(Comparator.comparingInt(TopicPartition::partition))
                    .toList()) {
                long from = start.get(tp);
                long until = readable.get(tp);
                next.put(tp, from);
                if (from >= until || messages.size() >= request.limit() || bytes >= MAX_PAGE_BYTES) {
                    continue;
                }
                consumer.assign(List.of(tp));
                consumer.seek(tp, from);
                while (consumer.position(tp) < until
                        && messages.size() < request.limit()
                        && bytes < MAX_PAGE_BYTES
                        && !deadline.expired(clock)) {
                    Duration wait = POLL.compareTo(deadline.remaining(clock)) < 0 ? POLL : deadline.remaining(clock);
                    ConsumerRecords<byte[], byte[]> polled = consumer.poll(wait);
                    boolean cut = false;
                    for (ConsumerRecord<byte[], byte[]> record : polled.records(tp)) {
                        if (record.offset() >= until || messages.size() >= request.limit() || bytes >= MAX_PAGE_BYTES) {
                            cut = true;
                            break;
                        }
                        Message message = RecordCodec.message(source, record, request.maxBodyBytes());
                        messages.add(message);
                        bytes += message.body().size();
                        next.put(tp, record.offset() + 1);
                    }
                    if (cut) {
                        break;
                    }
                    // Every record polled was taken; those skipped (transaction markers, a compacted gap) move
                    // the position without a message, and the next page starts after them.
                    next.put(tp, Math.max(next.get(tp), Math.min(consumer.position(tp), until)));
                }
            }
            long remaining = 0;
            for (TopicPartition tp : chosen) {
                remaining += Math.max(0, readable.get(tp) - next.get(tp));
            }
            Optional<Cursor> cursor = remaining > 0 ? Optional.of(cursorOf(next)) : Optional.empty();
            long total = 0;
            Map<TopicPartition, Long> log = Offsets.earliest(context, chosen);
            for (TopicPartition tp : chosen) {
                total += Math.max(0, readable.get(tp) - log.get(tp));
            }
            return new BrowsePage(messages, cursor, new Total(Denominator.ALL, Count.estimate(total)));
        }
    }

    /** Where a read-committed consumer may read to: the last stable offset of each partition. */
    private static Map<TopicPartition, Long> readableEnd(
            KafkaConsumer<byte[], byte[]> consumer, Collection<TopicPartition> partitions) {
        return new HashMap<>(consumer.endOffsets(partitions));
    }

    private static Map<TopicPartition, Long> start(
            Context context, Collection<TopicPartition> partitions, Map<TopicPartition, Long> end, Position position) {
        Map<TopicPartition, Long> first = Offsets.earliest(context, partitions);
        Map<TopicPartition, Long> out = new HashMap<>(first);
        switch (position) {
            case Position.Earliest e -> {}
            case Position.Latest l -> out.putAll(end);
            case Position.AtTime t ->
                out.putAll(Offsets.atTime(context, partitions, t.time().toEpochMilli()));
            case Position.AtOffsets at -> {
                Map<TopicPartition, Long> named = Seeks.resolve(context, partitions, at);
                out.putAll(named);
            }
            case Position.After after -> out.putAll(parse(after.cursor(), partitions, first, end));
            case Position.AtSnapshot s ->
                throw KafkaErrors.refused(
                        Status.UNIMPLEMENTED, ErrorReasons.NOT_SUPPORTED, "Kafka has no snapshots to browse.");
        }
        return out;
    }

    private static Cursor cursorOf(Map<TopicPartition, Long> next) {
        StringBuilder token = new StringBuilder();
        next.forEach((tp, offset) -> {
            if (!token.isEmpty()) {
                token.append(',');
            }
            token.append(tp.partition()).append(':').append(offset);
        });
        return new Cursor(token.toString());
    }

    private static Map<TopicPartition, Long> parse(
            Cursor cursor,
            Collection<TopicPartition> partitions,
            Map<TopicPartition, Long> first,
            Map<TopicPartition, Long> end) {
        Map<TopicPartition, Long> out = new HashMap<>();
        try {
            for (String part : cursor.token().split(",")) {
                String[] pair = part.split(":");
                int number = Integer.parseInt(pair[0]);
                long offset = Long.parseLong(pair[1]);
                TopicPartition tp = partitions.stream()
                        .filter(p -> p.partition() == number)
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("partition " + number));
                // A log that was truncated or replaced since the cursor was made is read from where it now starts.
                out.put(tp, Math.min(Math.max(offset, first.get(tp)), end.get(tp)));
            }
        } catch (RuntimeException e) {
            throw Attrs.invalid("Not a cursor of this topic: " + cursor.token());
        }
        return out;
    }

    /** The log's size in records, as an estimate: its end less its start, over the partitions. */
    static Count size(Context context, Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> first = Offsets.earliest(context, partitions);
        Map<TopicPartition, Long> last = Offsets.latest(context, partitions);
        long total = 0;
        for (TopicPartition tp : partitions) {
            total += Math.max(0, last.get(tp) - first.get(tp));
        }
        return Count.estimate(total);
    }
}
