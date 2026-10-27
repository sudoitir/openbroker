package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import java.nio.file.Path;
import java.time.Duration;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The starter's own keys. The connections under {@code openbroker.connections.<name>} bind to
 * {@link Connection} instead, by {@link RecordBinder}.
 *
 * @param client the connect options every connection starts from
 * @param tracing how sends are traced
 * @param springConnections connections derived from Spring Boot's Kafka and Artemis connection details
 * @param health the health indicators
 */
@ConfigurationProperties("openbroker")
public record OpenBrokerProperties(
        @DefaultValue Client client,
        @DefaultValue Tracing tracing,
        @DefaultValue SpringConnections springConnections,
        @DefaultValue Health health) {

    /**
     * The connect options every connection starts from; the SDK's own defaults fill what is not set.
     *
     * @param connectTimeout how long a connection attempt may take
     * @param callTimeout how long a call may take
     * @param maxResponseBytes the largest response the client reads
     * @param spoolDirectory where the client spools large bodies
     */
    public record Client(
            @Nullable Duration connectTimeout,
            @Nullable Duration callTimeout,
            @Nullable Long maxResponseBytes,
            @Nullable Path spoolDirectory) {}

    /**
     * How sends are traced.
     *
     * @param injectContext whether a send writes the trace context into the message; turn it off in a
     *     tool that moves or relays messages other producers wrote
     */
    public record Tracing(@DefaultValue("true") boolean injectContext) {}

    /**
     * Connections derived from Spring Boot's connection details.
     *
     * @param enabled whether a {@code kafka} or {@code artemis} connection is derived from them
     */
    public record SpringConnections(@DefaultValue("true") boolean enabled) {}

    /**
     * The health indicators.
     *
     * @param timeout how long a check of one connection may take before it is down
     */
    public record Health(@DefaultValue("2s") Duration timeout) {}
}
