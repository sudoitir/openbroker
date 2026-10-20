package io.github.sudoitir.openbroker.access;

import java.util.Objects;
import java.util.Optional;

/** Whether a capability is available, why, and how to make it so. Unknown is not unavailable. */
public record Assessment(Status status, String reason, Optional<Remedy> remedy) {

    public enum Status {
        AVAILABLE,
        UNAVAILABLE,
        UNKNOWN
    }

    public Assessment {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(remedy, "remedy");
    }

    public static Assessment available(String reason) {
        return new Assessment(Status.AVAILABLE, reason, Optional.empty());
    }

    public static Assessment unavailable(String reason, Optional<Remedy> remedy) {
        return new Assessment(Status.UNAVAILABLE, reason, remedy);
    }

    public static Assessment unknown(String reason) {
        return new Assessment(Status.UNKNOWN, reason, Optional.empty());
    }
}
