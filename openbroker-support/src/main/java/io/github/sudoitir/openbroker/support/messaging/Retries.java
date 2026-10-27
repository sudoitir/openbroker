package io.github.sudoitir.openbroker.support.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.RetryMode;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SystemRetry;

/** Resolves a processor's retry policy against what the system enforces. */
public final class Retries {

    private Retries() {}

    /**
     * The policy a processor runs with. Every driver calls this when it opens a processor, before its
     * own checks of the policy.
     *
     * <ul>
     *   <li>{@code DEFAULT} becomes {@code SYSTEM} where the system counts attempts, and otherwise
     *       {@code MANAGED} with the policy's attempts and backoff (10 and the default backoff for
     *       {@code RetryPolicy.defaults()}). A driver retries that managed default in place, so it
     *       needs no retry destinations.
     *   <li>{@code SYSTEM} where the system counts no attempts is refused: nothing would give up.
     *   <li>{@code MANAGED} is kept; it is always available, and may always name a dead-letter target.
     * </ul>
     *
     * @throws BrokerException {@code UNIMPLEMENTED}, not sent, for {@code SYSTEM} where nothing is counted
     */
    public static RetryPolicy resolve(RetryPolicy policy, SystemRetry system) {
        return switch (policy.mode()) {
            case DEFAULT ->
                system.countsAttempts()
                        ? RetryPolicy.system()
                        : new RetryPolicy(
                                RetryMode.MANAGED, policy.maxAttempts(), policy.backoff(), policy.deadLetter());
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
                yield policy;
            }
            case MANAGED -> policy;
        };
    }
}
