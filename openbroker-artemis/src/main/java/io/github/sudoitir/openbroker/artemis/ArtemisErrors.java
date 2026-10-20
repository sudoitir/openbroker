package io.github.sudoitir.openbroker.artemis;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.artemis.internal.Errors;
import java.util.Optional;

/**
 * How the driver reads a refused management operation, for callers of {@link ArtemisJolokia}: a
 * known Artemis error code becomes the SDK's status and reason (an absent queue is {@code
 * RESOURCE_GONE}, consumers attached are {@code HAS_CONSUMERS}), with the broker's text and code in
 * the metadata under {@value #NATIVE_ERROR} and {@value #NATIVE_CODE}.
 */
public final class ArtemisErrors {

    /** Metadata key: the broker's own error text. */
    public static final String NATIVE_ERROR = Errors.NATIVE_ERROR;

    /** Metadata key: the broker's error code ({@code AMQ229017}) or Java class name. */
    public static final String NATIVE_CODE = Errors.NATIVE_CODE;

    /** Metadata key: the address-setting a parse error names. */
    public static final String SETTING = Errors.SETTING;

    private ArtemisErrors() {}

    /** The failure for a response the broker refused; empty when it succeeded. */
    public static Optional<BrokerException> refusal(JolokiaResponse response, String operation) {
        return response.ok() ? Optional.empty() : Optional.of(Errors.refusal(null, response, operation));
    }
}
