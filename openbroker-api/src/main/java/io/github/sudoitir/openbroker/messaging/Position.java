package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Where to start reading, or where to seek or truncate to. */
public sealed interface Position {

    /** The oldest message retained. */
    record Earliest() implements Position {}

    /** After the newest message: only messages that arrive later. */
    record Latest() implements Position {}

    /** An offset per shard, for systems with offsets. */
    record AtOffsets(Map<Shard, Long> offsets) implements Position {
        public AtOffsets {
            offsets = Map.copyOf(offsets);
        }
    }

    /** The first message produced at or after this time. */
    record AtTime(Instant time) implements Position {
        public AtTime {
            Objects.requireNonNull(time, "time");
        }
    }

    /** Where a previous read left off. */
    record After(Cursor cursor) implements Position {
        public After {
            Objects.requireNonNull(cursor, "cursor");
        }
    }

    /** A snapshot the system keeps, such as a Pub/Sub snapshot. */
    record AtSnapshot(ResourceId snapshot) implements Position {
        public AtSnapshot {
            Objects.requireNonNull(snapshot, "snapshot");
        }
    }

    static Position earliest() {
        return new Earliest();
    }

    static Position latest() {
        return new Latest();
    }

    static Position after(Cursor cursor) {
        return new After(cursor);
    }
}
