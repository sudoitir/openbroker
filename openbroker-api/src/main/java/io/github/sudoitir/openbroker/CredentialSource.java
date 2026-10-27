package io.github.sudoitir.openbroker;

/**
 * Supplies the account for a plane. A driver asks on every connect and reconnect, never once at
 * start, so a rotated secret takes effect on the next connection without reconnecting the rest.
 */
@FunctionalInterface
public interface CredentialSource {

    Credentials forPlane(Plane plane);

    /** The same account for every plane. */
    static CredentialSource of(Credentials credentials) {
        return plane -> credentials;
    }

    static CredentialSource none() {
        return plane -> new Credentials.None();
    }
}
