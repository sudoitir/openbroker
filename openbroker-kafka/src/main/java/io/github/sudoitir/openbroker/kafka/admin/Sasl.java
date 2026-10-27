package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;

/**
 * The SASL settings of a Kafka client. The login module carries no secret: the client asks the
 * credential source at each authentication, through the callback handler.
 */
final class Sasl {

    private Sasl() {}

    /**
     * The JAAS configuration of the login module for a mechanism.
     *
     * @throws io.github.sudoitir.openbroker.BrokerException with {@code INVALID_ARGUMENT} when the
     *     mechanism is not supported
     */
    static String loginModule(String mechanism) {
        String module =
                switch (mechanism) {
                    case KafkaOptions.PLAIN -> "org.apache.kafka.common.security.plain.PlainLoginModule";
                    case KafkaOptions.SCRAM_SHA_256, KafkaOptions.SCRAM_SHA_512 ->
                        "org.apache.kafka.common.security.scram.ScramLoginModule";
                    case KafkaOptions.OAUTHBEARER ->
                        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";
                    default ->
                        throw KafkaErrors.refused(
                                Status.INVALID_ARGUMENT,
                                ErrorReasons.INVALID_ARGUMENT,
                                "The SASL mechanism " + mechanism + " is not supported.");
                };
        return module + " required;";
    }

    /**
     * Checks that the credentials are the form the mechanism takes, so a mismatch fails when the
     * client is configured, naming the form, rather than at the first authentication.
     *
     * @throws io.github.sudoitir.openbroker.BrokerException with {@code INVALID_ARGUMENT} when they are not
     */
    static void requireForm(String mechanism, Credentials credentials) {
        boolean token = mechanism.equals(KafkaOptions.OAUTHBEARER);
        boolean fits = token
                ? credentials instanceof Credentials.BearerToken
                : credentials instanceof Credentials.UsernamePassword;
        if (!fits) {
            throw KafkaErrors.refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    "SASL " + mechanism + " takes " + (token ? "a bearer token" : "a user name and password")
                            + " as credentials.");
        }
    }
}
