package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import java.nio.file.Path;
import java.time.Duration;
import org.jspecify.annotations.Nullable;

/**
 * What configuration can say of the SDK's {@code ConnectOptions}: the values that bind. Each key that
 * is set is laid over the client defaults ({@code openbroker.client.*}); the function-valued options
 * come from beans.
 *
 * @param connectTimeout how long a connection attempt may take
 * @param callTimeout how long a call may take
 * @param maxResponseBytes the largest response the client reads
 * @param spoolDirectory where the client spools large bodies
 * @param sslBundle the name of the Spring SSL bundle that supplies TLS, if the connection uses TLS
 * @param verifyHostname whether the server's host name is checked against its certificate (default true)
 */
public record ConnectionOptions(
        @Nullable Duration connectTimeout,
        @Nullable Duration callTimeout,
        @Nullable Long maxResponseBytes,
        @Nullable Path spoolDirectory,
        @Nullable String sslBundle,
        @Nullable Boolean verifyHostname) {

    /** Nothing set: every value comes from the client defaults. */
    public static ConnectionOptions unset() {
        return new ConnectionOptions(null, null, null, null, null, null);
    }

    boolean hostnameVerified() {
        return verifyHostname == null || verifyHostname;
    }
}
