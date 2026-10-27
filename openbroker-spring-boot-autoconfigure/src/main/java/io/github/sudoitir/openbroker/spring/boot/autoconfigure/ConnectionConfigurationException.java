package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import org.jspecify.annotations.Nullable;

/**
 * A connection's configuration is wrong. It names the connection and, where there is one, the key, and
 * it never carries a value: the failure analyzer prints it as it is.
 */
public final class ConnectionConfigurationException extends RuntimeException {

    private final String connection;
    private final @Nullable String key;
    private final String problem;

    ConnectionConfigurationException(String connection, @Nullable String key, String problem) {
        this(connection, key, problem, null);
    }

    ConnectionConfigurationException(
            String connection, @Nullable String key, String problem, @Nullable Throwable cause) {
        super(describe(connection, key, problem), cause);
        this.connection = connection;
        this.key = key;
        this.problem = problem;
    }

    /** The connection's name. */
    public String connection() {
        return connection;
    }

    /** The configuration key, such as {@code openbroker.connections.orders.processor.concurrency}. */
    public @Nullable String key() {
        return key;
    }

    /** What is wrong, without the connection or the key. */
    public String problem() {
        return problem;
    }

    private static String describe(String connection, @Nullable String key, String problem) {
        return "Connection '" + connection + "'" + (key == null ? "" : ", " + key) + ": " + problem;
    }
}
