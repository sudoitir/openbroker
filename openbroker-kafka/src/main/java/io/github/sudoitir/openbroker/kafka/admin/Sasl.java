package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;

/** The SASL settings of a Kafka client for a form of credentials. */
final class Sasl {

    private Sasl() {}

    /**
     * The JAAS configuration that logs a client in with a user name and password.
     *
     * @throws io.github.sudoitir.openbroker.BrokerException with {@code INVALID_ARGUMENT} when the
     *     mechanism takes no password
     */
    static String jaasConfig(String mechanism, Credentials credentials) {
        if (!(credentials instanceof Credentials.UsernamePassword account)) {
            throw KafkaErrors.refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    "SASL " + mechanism + " takes a user name and password as credentials.");
        }
        String module =
                switch (mechanism) {
                    case KafkaOptions.PLAIN -> "org.apache.kafka.common.security.plain.PlainLoginModule";
                    case KafkaOptions.SCRAM_SHA_256, KafkaOptions.SCRAM_SHA_512 ->
                        "org.apache.kafka.common.security.scram.ScramLoginModule";
                    default ->
                        throw KafkaErrors.refused(
                                Status.INVALID_ARGUMENT,
                                ErrorReasons.INVALID_ARGUMENT,
                                "The SASL mechanism " + mechanism + " is not supported.");
                };
        return module + " required username=\"" + quote(account.username()) + "\" password=\""
                + quote(account.password().reveal()) + "\";";
    }

    /** Escapes a value for a double-quoted JAAS option. */
    private static String quote(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
