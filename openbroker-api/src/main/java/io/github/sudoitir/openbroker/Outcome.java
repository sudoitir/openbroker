package io.github.sudoitir.openbroker;

import java.util.Optional;

/** How a call ended, as reported on its {@link Permit}. */
public record Outcome(boolean success, Optional<BrokerException> failure) {

    private static final Outcome SUCCESS = new Outcome(true, Optional.empty());

    public static Outcome succeeded() {
        return SUCCESS;
    }

    public static Outcome failed(BrokerException failure) {
        return new Outcome(false, Optional.of(failure));
    }
}
