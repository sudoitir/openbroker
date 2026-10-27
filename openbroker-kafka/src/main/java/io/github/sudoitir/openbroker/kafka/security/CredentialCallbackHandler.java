package io.github.sudoitir.openbroker.kafka.security;

import io.github.sudoitir.openbroker.Credentials;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.scram.ScramExtensionsCallback;
import org.jspecify.annotations.Nullable;

/**
 * Answers the SASL callbacks of PLAIN, SCRAM and OAUTHBEARER with the credentials the connection's
 * source holds <em>now</em>: it asks the source each time a client authenticates, so a password or
 * token that was rotated is used by the next connection, and nothing is kept between asks.
 *
 * <p>Read from the client configuration under {@link #CREDENTIALS}: a {@link Supplier} of the
 * plane's {@link Credentials}. Its text never shows a secret.
 */
public final class CredentialCallbackHandler implements AuthenticateCallbackHandler {

    /** The client configuration key that holds the {@code Supplier<Credentials>}. */
    public static final String CREDENTIALS = "openbroker.credentials";

    private static final String PRINCIPAL = "openbroker";

    private Supplier<Credentials> credentials = () -> new Credentials.None();

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        Object supplied = configs.get(CREDENTIALS);
        if (!(supplied instanceof Supplier<?>)) {
            throw new IllegalArgumentException("The client configuration carries no " + CREDENTIALS);
        }
        this.credentials = (Supplier<Credentials>) supplied;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            switch (callback) {
                case NameCallback name -> name.setName(account().username());
                case PasswordCallback password ->
                    password.setPassword(account().password().reveal().toCharArray());
                case OAuthBearerTokenCallback token -> token.token(bearer());
                case ScramExtensionsCallback ignored -> {}
                default -> throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private Credentials.UsernamePassword account() throws IOException {
        if (credentials.get() instanceof Credentials.UsernamePassword account) {
            return account;
        }
        throw new IOException("The credential source no longer gives a user name and password");
    }

    private OAuthBearerToken bearer() throws IOException {
        if (credentials.get() instanceof Credentials.BearerToken token) {
            return new Token(token.token().reveal(), token.expiresAt());
        }
        throw new IOException("The credential source no longer gives a bearer token");
    }

    @Override
    public void close() {}

    /** A bearer token as Kafka's OAUTHBEARER mechanism reads it; its text never shows the token. */
    private record Token(String value, Instant expiresAt) implements OAuthBearerToken {

        Token {
            Objects.requireNonNull(value, "value");
        }

        @Override
        public Set<String> scope() {
            return Set.of();
        }

        @Override
        public long lifetimeMs() {
            return expiresAt.equals(Instant.MAX) ? Long.MAX_VALUE : expiresAt.toEpochMilli();
        }

        @Override
        public String principalName() {
            return PRINCIPAL;
        }

        @Override
        public @Nullable Long startTimeMs() {
            return null;
        }

        @Override
        public String toString() {
            return "Token[redacted]";
        }
    }
}
