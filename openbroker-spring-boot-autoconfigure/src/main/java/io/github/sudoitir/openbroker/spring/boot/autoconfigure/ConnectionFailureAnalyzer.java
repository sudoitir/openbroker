package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/** Reports a wrong connection by its name and key, never by a value. */
final class ConnectionFailureAnalyzer extends AbstractFailureAnalyzer<ConnectionConfigurationException> {

    @Override
    protected FailureAnalysis analyze(Throwable rootFailure, ConnectionConfigurationException cause) {
        return new FailureAnalysis(
                cause.getMessage(),
                "Correct " + (cause.key() == null ? "connection '" + cause.connection() + "'" : cause.key())
                        + " in the application's configuration.",
                cause);
    }
}
