package io.github.sudoitir.openbroker.model;

import java.util.Objects;
import java.util.Optional;

/**
 * A node's place in a high-availability group: the group, its configured designation, whether it
 * is serving now, and whether its replica is in sync. A backup that took over reads {@code BACKUP}
 * and {@code ACTIVE}.
 */
public record Membership(Optional<String> group, Designation designation, Serving serving, Optional<Boolean> inSync) {

    public Membership {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(designation, "designation");
        Objects.requireNonNull(serving, "serving");
        Objects.requireNonNull(inSync, "inSync");
    }

    public static Membership standalone(Serving serving) {
        return new Membership(Optional.empty(), Designation.NONE, serving, Optional.empty());
    }
}
