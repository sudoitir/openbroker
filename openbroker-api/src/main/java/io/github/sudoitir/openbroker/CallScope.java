package io.github.sudoitir.openbroker;

import java.util.Optional;

/**
 * Per-call context a caller binds around a facet call. A driver reads it once when the call enters,
 * and carries it explicitly onto its own threads.
 *
 * <pre>{@code
 * ScopedValue.where(CallScope.DEADLINE, Deadline.after(Duration.ofSeconds(2), clock))
 *         .call(() -> connection.resources().read(query));
 * }</pre>
 */
public final class CallScope {

    /** The deadline of the calls made inside the scope; without it, the connection's call timeout. */
    public static final ScopedValue<Deadline> DEADLINE = ScopedValue.newInstance();

    private CallScope() {}

    public static Optional<Deadline> deadline() {
        return DEADLINE.isBound() ? Optional.of(DEADLINE.get()) : Optional.empty();
    }
}
