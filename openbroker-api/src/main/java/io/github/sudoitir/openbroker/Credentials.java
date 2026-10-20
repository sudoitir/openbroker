package io.github.sudoitir.openbroker;

import java.util.Objects;

/** An account name and its secret. {@code toString()} never shows the secret. */
public record Credentials(String username, Secret password) {

    private static final Credentials NONE = new Credentials("", Secret.empty());

    public Credentials {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
    }

    public static Credentials of(String username, String password) {
        return new Credentials(username, Secret.of(password));
    }

    /** No account: the endpoint is reached anonymously. */
    public static Credentials none() {
        return NONE;
    }

    public boolean isNone() {
        return username.isEmpty() && password.isEmpty();
    }
}
