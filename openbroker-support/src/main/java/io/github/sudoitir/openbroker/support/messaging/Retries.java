package io.github.sudoitir.openbroker.support.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.RetryMode;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SystemRetry;

/** Resolves a processor's retry policy against what the system enforces. */
public final class Retries {

    private Retries() {}

    /**
     * A resolved policy and where a managed retry runs.
     *
     * @param policy the policy to run with and to report; never {@code DEFAULT}
     * @param inPlace whether a failed message is retried where it is, holding its lane through the
     *     backoff: true for an ordered processor and for a resolved default, so a default processor
     *     needs no retry destinations
     */
    public record Resolved(RetryPolicy policy, boolean inPlace) {}

    /**
     * The policy a processor runs with. Every driver calls this when it opens a processor, before its
     * own checks of the policy.
     *
     * <ul>
     *   <li>{@code DEFAULT} becomes {@code SYSTEM} where the system counts attempts, and otherwise
     *       {@code MANAGED} with the policy's attempts and backoff (10 and the default backoff for
     *       {@code RetryPolicy.defaults()}), retried in place whatever the ordering.
     *   <li>{@code SYSTEM} where the system counts no attempts is refused: nothing would give up.
     *   <li>{@code MANAGED} is kept; it is always available, and may always name a dead-letter target.
     * </ul>
     *
     * @throws BrokerException {@code UNIMPLEMENTED}, not sent, for {@code SYSTEM} where nothing is counted
     */
    public static Resolved resolve(RetryPolicy policy, SystemRetry system, OrderingScope ordering) {
        boolean ordered = ordering != OrderingScope.NONE;
        return switch (policy.mode()) {
            case DEFAULT ->
                system.countsAttempts()
                        ? new Resolved(RetryPolicy.system(), ordered)
                        : new Resolved(
                                new RetryPolicy(
                                        RetryMode.MANAGED, policy.maxAttempts(), policy.backoff(), policy.deadLetter()),
                                true);
            case SYSTEM -> {
                if (!system.countsAttempts()) {
                    throw new BrokerException(
                            Status.UNIMPLEMENTED,
                            ErrorInfo.of(ErrorReasons.NOT_SUPPORTED),
                            Dispatch.NOT_SENT,
                            null,
                            "The system counts no delivery attempts, so RetryMode.SYSTEM would never give up. "
                                    + "Use RetryPolicy.managed(...), or RetryPolicy.defaults() to take what the system offers.");
                }
                yield new Resolved(policy, ordered);
            }
            case MANAGED -> new Resolved(policy, ordered);
        };
    }
}
