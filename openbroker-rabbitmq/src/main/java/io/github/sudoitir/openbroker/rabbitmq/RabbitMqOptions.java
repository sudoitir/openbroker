package io.github.sudoitir.openbroker.rabbitmq;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionSchema;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.OptionSpec;
import io.github.sudoitir.openbroker.Status;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;

/**
 * The connection options of the RabbitMQ driver, keyed as in {@link #SCHEMA}, and the protocol names of its
 * endpoints: the management API is reached at {@code http} or {@code https} endpoints on the {@code MANAGEMENT}
 * plane, and messages at {@code amqp} or {@code amqps} endpoints on the {@code DATA} plane.
 */
public final class RabbitMqOptions {

    /** The protocol of a management endpoint in the clear. */
    public static final String HTTP = "http";
    /** The protocol of a management endpoint over TLS. */
    public static final String HTTPS = "https";
    /** The protocol of an AMQP 0-9-1 endpoint in the clear. */
    public static final String AMQP = "amqp";
    /** The protocol of an AMQP 0-9-1 endpoint over TLS. */
    public static final String AMQPS = "amqps";

    /** The polling period of the watch, and the age of the shared queue listing, as an ISO-8601 duration. */
    public static final String WATCH_INTERVAL = "rabbitmq.watch_interval";
    /** The virtual host that holds the event exchange {@code amq.rabbitmq.event}. */
    public static final String EVENT_VHOST = "rabbitmq.event_vhost";

    public static final String DEFAULT_WATCH_INTERVAL = "PT10S";
    public static final String DEFAULT_EVENT_VHOST = "/";

    /** The shortest watch interval. */
    public static final Duration MIN_WATCH_INTERVAL = Duration.ofSeconds(1);

    public static final ConnectionSchema SCHEMA = new ConnectionSchema(List.of(
            new OptionSpec(
                    WATCH_INTERVAL,
                    OptionSpec.Type.DURATION,
                    false,
                    false,
                    Optional.of(DEFAULT_WATCH_INTERVAL),
                    List.of(),
                    "How often the watch reads the cluster, as an ISO-8601 duration of one second or more."
                            + " It is also how old the shared queue listing may get.",
                    Optional.empty()),
            new OptionSpec(
                    EVENT_VHOST,
                    OptionSpec.Type.STRING,
                    false,
                    false,
                    Optional.of(DEFAULT_EVENT_VHOST),
                    List.of(),
                    "The virtual host where the event exchange lives, which events are read from when the plugin"
                            + " and the permissions allow it.",
                    Optional.empty())));

    private RabbitMqOptions() {}

    /** The watch interval of {@code spec}, at least {@link #MIN_WATCH_INTERVAL}. */
    public static Duration watchInterval(ClusterSpec spec) {
        String text = spec.option(WATCH_INTERVAL, DEFAULT_WATCH_INTERVAL);
        try {
            Duration interval = Duration.parse(text);
            if (interval.compareTo(MIN_WATCH_INTERVAL) >= 0) {
                return interval;
            }
        } catch (DateTimeParseException e) {
            throw invalid(text, e);
        }
        throw invalid(text, null);
    }

    /** The virtual host of the event exchange of {@code spec}. */
    public static String eventVhost(ClusterSpec spec) {
        return spec.option(EVENT_VHOST, DEFAULT_EVENT_VHOST);
    }

    private static BrokerException invalid(String text, DateTimeParseException cause) {
        return new BrokerException(
                Status.INVALID_ARGUMENT,
                ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT),
                Dispatch.NOT_SENT,
                null,
                WATCH_INTERVAL + " is an ISO-8601 duration of one second or more, such as PT10S, not '" + text + "'.",
                cause);
    }
}
