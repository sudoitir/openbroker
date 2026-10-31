package io.github.sudoitir.openbroker.kafka;

import io.github.sudoitir.openbroker.ConnectionSchema;
import io.github.sudoitir.openbroker.OptionSpec;
import java.util.List;
import java.util.Optional;

/**
 * The connection options of the Kafka driver, keyed as in {@link #SCHEMA}. The bootstrap servers
 * are the cluster's {@code DATA} endpoints with protocol {@value #PROTOCOL}, as {@code host:port}.
 */
public final class KafkaOptions {

    /** The protocol of an endpoint that is a bootstrap server. */
    public static final String PROTOCOL = "kafka";

    public static final String SECURITY_PROTOCOL = "security.protocol";
    public static final String SASL_MECHANISM = "sasl.mechanism";
    public static final String CLIENT_ID = "client.id";
    /** How often the watch reads the cluster, as an ISO-8601 duration such as {@code PT10S}. */
    public static final String WATCH_INTERVAL = "watch.interval";
    /**
     * How long building a SASL channel may take once the socket is open, as an ISO-8601 duration. Kafka charges a
     * broker's name lookup and login to its connection setup, so its own 10 s default is kept when the caller's
     * connect timeout is shorter; this option raises or lowers that floor.
     */
    public static final String SETUP_GRACE = "setup.grace";

    public static final String PLAINTEXT = "PLAINTEXT";
    public static final String SSL = "SSL";
    public static final String SASL_PLAINTEXT = "SASL_PLAINTEXT";
    public static final String SASL_SSL = "SASL_SSL";

    public static final String PLAIN = "PLAIN";
    public static final String SCRAM_SHA_256 = "SCRAM-SHA-256";
    public static final String SCRAM_SHA_512 = "SCRAM-SHA-512";
    public static final String OAUTHBEARER = "OAUTHBEARER";

    public static final String DEFAULT_CLIENT_ID = "openbroker";
    public static final String DEFAULT_WATCH_INTERVAL = "PT10S";

    /**
     * Kafka's own default for {@code socket.connection.setup.timeout.ms}. A client built with less than this
     * abandons a socket that is still naming its broker or logging in, which a slow reverse DNS or a stopped
     * policy server makes ordinary; Kafka then disconnects before it reads a single API version.
     */
    public static final String DEFAULT_SETUP_GRACE = "PT10S";

    public static final ConnectionSchema SCHEMA = new ConnectionSchema(List.of(
            new OptionSpec(
                    SECURITY_PROTOCOL,
                    OptionSpec.Type.STRING,
                    false,
                    false,
                    Optional.of(PLAINTEXT),
                    List.of(PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL),
                    "How the clients reach the brokers: in the clear or over TLS, with or without SASL.",
                    Optional.empty()),
            new OptionSpec(
                    SASL_MECHANISM,
                    OptionSpec.Type.STRING,
                    false,
                    false,
                    Optional.of(PLAIN),
                    List.of(PLAIN, SCRAM_SHA_256, SCRAM_SHA_512, OAUTHBEARER),
                    "The SASL mechanism, read when the security protocol is a SASL one.",
                    Optional.empty()),
            new OptionSpec(
                    CLIENT_ID,
                    OptionSpec.Type.STRING,
                    false,
                    false,
                    Optional.of(DEFAULT_CLIENT_ID),
                    List.of(),
                    "The client id the brokers see in their logs and quotas.",
                    Optional.empty()),
            new OptionSpec(
                    WATCH_INTERVAL,
                    OptionSpec.Type.DURATION,
                    false,
                    false,
                    Optional.of(DEFAULT_WATCH_INTERVAL),
                    List.of(),
                    "How often the watch reads the cluster, as an ISO-8601 duration of one second or more."
                            + " Kafka sends no events, so changes show up this late at most.",
                    Optional.empty()),
            new OptionSpec(
                    SETUP_GRACE,
                    OptionSpec.Type.DURATION,
                    false,
                    false,
                    Optional.of(DEFAULT_SETUP_GRACE),
                    List.of(),
                    "How long building a connection may take once its socket is open. Kafka counts naming the"
                            + " broker and logging in against it, so a slow reverse DNS ends the connection instead.",
                    Optional.empty())));

    private KafkaOptions() {}
}
