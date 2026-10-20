package io.github.sudoitir.openbroker.messaging;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Which messages a bulk action applies to. */
public sealed interface Selection {

    record ById(Set<MessageRef> refs) implements Selection {
        public ById {
            refs = Set.copyOf(refs);
        }
    }

    /**
     * The messages matching a filter, and, when bounded, only those produced at or before that time,
     * so a selection over a queue still being produced to has an end.
     */
    record ByFilter(MessageFilter filter, Optional<Instant> producedAtOrBefore) implements Selection {
        public ByFilter {
            Objects.requireNonNull(filter, "filter");
            Objects.requireNonNull(producedAtOrBefore, "producedAtOrBefore");
        }
    }

    /** Every message, optionally bounded by production time. */
    record All(Optional<Instant> producedAtOrBefore) implements Selection {
        public All {
            Objects.requireNonNull(producedAtOrBefore, "producedAtOrBefore");
        }
    }
}
