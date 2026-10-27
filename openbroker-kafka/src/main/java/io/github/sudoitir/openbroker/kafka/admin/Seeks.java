package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;

/** Where a position falls on the partitions of a topic. */
final class Seeks {

    private Seeks() {}

    /** The partitions a scope names among a topic's: all of them for the whole, those it lists for shards. */
    static Set<TopicPartition> partitionsIn(Scope scope, Collection<TopicPartition> all) {
        if (scope instanceof Scope.Whole) {
            return new LinkedHashSet<>(all);
        }
        Set<TopicPartition> out = new LinkedHashSet<>();
        for (Shard shard : ((Scope.Shards) scope).shards()) {
            if (!(shard instanceof Shard.Partition named)) {
                throw Attrs.invalid(
                        "Scope Kafka work to partitions of the topic; a node holds some of its partitions, not a share of its log.");
            }
            int number = number(named);
            out.add(all.stream()
                    .filter(tp -> tp.partition() == number)
                    .findFirst()
                    .orElseThrow(() -> KafkaErrors.refused(
                            Status.NOT_FOUND,
                            ErrorReasons.RESOURCE_GONE,
                            "The topic has no partition " + number + ".")));
        }
        return out;
    }

    static int number(Shard.Partition shard) {
        try {
            return Integer.parseInt(shard.partition().value());
        } catch (NumberFormatException e) {
            throw Attrs.invalid("A Kafka partition is named by its number: " + shard.partition());
        }
    }

    /**
     * The offset the position names on each partition it names: the earliest or latest of every
     * partition in {@code partitions}, the first offset at or after a time (or the end where none is),
     * or the offsets given for the partitions given.
     */
    static Map<TopicPartition, Long> resolve(
            Context context, Collection<TopicPartition> partitions, Position position) {
        return switch (position) {
            case Position.Earliest e -> Offsets.earliest(context, partitions);
            case Position.Latest l -> Offsets.latest(context, partitions);
            case Position.AtTime t ->
                Offsets.atTime(context, partitions, t.time().toEpochMilli());
            case Position.AtOffsets at -> atOffsets(context, partitions, at);
            case Position.After a ->
                throw KafkaErrors.refused(
                        Status.UNIMPLEMENTED,
                        ErrorReasons.NOT_SUPPORTED,
                        "A Kafka log has no cursor to continue after; name offsets, a time, the earliest or the latest.");
            case Position.AtSnapshot s ->
                throw KafkaErrors.refused(
                        Status.UNIMPLEMENTED, ErrorReasons.NOT_SUPPORTED, "Kafka has no snapshots to seek to.");
        };
    }

    private static Map<TopicPartition, Long> atOffsets(
            Context context, Collection<TopicPartition> partitions, Position.AtOffsets at) {
        Map<TopicPartition, Long> named =
                new TreeMap<>(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
        at.offsets().forEach((shard, offset) -> {
            if (!(shard instanceof Shard.Partition p)) {
                throw Attrs.invalid("Offsets in Kafka are named by partition.");
            }
            int number = number(p);
            TopicPartition tp = partitions.stream()
                    .filter(candidate -> candidate.partition() == number)
                    .findFirst()
                    .orElseThrow(() -> Attrs.invalid("The topic has no partition " + number + "."));
            named.put(tp, offset);
        });
        Map<TopicPartition, Long> start = Offsets.earliest(context, named.keySet());
        Map<TopicPartition, Long> end = Offsets.latest(context, named.keySet());
        named.forEach((tp, offset) -> {
            if (offset < start.get(tp) || offset > end.get(tp)) {
                throw KafkaErrors.refused(
                        Status.OUT_OF_RANGE,
                        ErrorReasons.INVALID_ARGUMENT,
                        "Offset " + offset + " is outside " + tp + ", which holds " + start.get(tp) + " to "
                                + end.get(tp) + ".");
            }
        });
        return named;
    }
}
