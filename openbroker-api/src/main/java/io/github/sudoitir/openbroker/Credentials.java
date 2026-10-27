package io.github.sudoitir.openbroker;

import java.time.Instant;
import java.util.Objects;

/**
 * How a plane proves who it is. {@code toString()} never shows a secret. A driver handles the forms it
 * supports and refuses the others with {@code INVALID_ARGUMENT}, naming the forms it accepts.
 */
public sealed interface Credentials {

    /** An account name and its secret. */
    record UsernamePassword(String username, Secret password) implements Credentials {

        public UsernamePassword {
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(password, "password");
        }
    }

    /**
     * A bearer token and the instant it stops being valid, {@link Instant#MAX} for a token that does
     * not expire. A source hands out a fresh token before the old one expires.
     */
    record BearerToken(Secret token, Instant expiresAt) implements Credentials {

        public BearerToken {
            Objects.requireNonNull(token, "token");
            Objects.requireNonNull(expiresAt, "expiresAt");
        }
    }

    /** No account: the endpoint is reached anonymously, or by mutual TLS alone. */
    record None() implements Credentials {}

    static UsernamePassword usernamePassword(String username, String password) {
        return new UsernamePassword(username, Secret.of(password));
    }

    static BearerToken bearerToken(String token, Instant expiresAt) {
        return new BearerToken(Secret.of(token), expiresAt);
    }
}
