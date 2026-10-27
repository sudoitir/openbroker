package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Secret;
import java.time.Instant;
import java.util.Locale;
import org.jspecify.annotations.Nullable;

/**
 * The credentials of a connection, as configuration says them. {@code form} names which of the SDK's
 * {@link Credentials} this is, and the keys of the other forms must stay unset.
 *
 * @param form {@code username-password}, {@code bearer-token} or {@code none}; unset when the
 *     connection sets no credentials
 * @param username the user name, for {@code username-password}
 * @param password the password, for {@code username-password}
 * @param token the bearer token, for {@code bearer-token}
 * @param expiresAt when the bearer token expires, for {@code bearer-token}
 */
public record CredentialsProperties(
        @Nullable Form form,
        @Nullable String username,
        @Nullable Secret password,
        @Nullable Secret token,
        @Nullable Instant expiresAt) {

    /** The forms the SDK's {@link Credentials} come in. */
    public enum Form {
        USERNAME_PASSWORD,
        BEARER_TOKEN,
        NONE
    }

    /** Credentials that are not set. */
    public static CredentialsProperties unset() {
        return new CredentialsProperties(null, null, null, null, null);
    }

    /** Whether the configuration says anything about credentials. */
    public boolean isSet() {
        return form != null;
    }

    /** The SDK's credentials; the caller checks {@link #isSet()} first. */
    Credentials toCredentials(String connection, String key) {
        Form chosen = form;
        if (chosen == null) {
            throw new ConnectionConfigurationException(connection, key, "name the form of the credentials");
        }
        return switch (chosen) {
            case USERNAME_PASSWORD -> {
                require(connection, key, "username", username != null);
                require(connection, key, "password", password != null);
                refuse(connection, key, "token", token == null && expiresAt == null);
                yield new Credentials.UsernamePassword(username, password);
            }
            case BEARER_TOKEN -> {
                require(connection, key, "token", token != null);
                require(connection, key, "expires-at", expiresAt != null);
                refuse(connection, key, "username or password", username == null && password == null);
                yield new Credentials.BearerToken(token, expiresAt);
            }
            case NONE -> {
                refuse(
                        connection,
                        key,
                        "username, password, token or expires-at",
                        username == null && password == null && token == null && expiresAt == null);
                yield new Credentials.None();
            }
        };
    }

    private void require(String connection, String key, String component, boolean present) {
        if (!present) {
            throw new ConnectionConfigurationException(
                    connection, key + "." + component, "is required for the form " + name());
        }
    }

    private void refuse(String connection, String key, String components, boolean absent) {
        if (!absent) {
            throw new ConnectionConfigurationException(
                    connection, key, components + " does not belong to the form " + name());
        }
    }

    private String name() {
        return form.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }
}
