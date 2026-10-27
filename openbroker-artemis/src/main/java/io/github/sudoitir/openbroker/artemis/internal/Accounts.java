package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Credentials.BearerToken;
import io.github.sudoitir.openbroker.Credentials.None;
import io.github.sudoitir.openbroker.Credentials.UsernamePassword;
import java.util.Optional;

/** The account a plane's credentials name. Artemis accepts a username and password, or none. */
final class Accounts {

    private Accounts() {}

    /** The username and password to present, or empty for an anonymous connection. */
    static Optional<UsernamePassword> account(Credentials credentials) {
        return switch (credentials) {
            case UsernamePassword account -> Optional.of(account);
            case None none -> Optional.empty();
            case BearerToken token ->
                throw Queries.invalid("Artemis accepts a username and password, or no credentials; "
                        + "it does not accept a bearer token.");
        };
    }
}
