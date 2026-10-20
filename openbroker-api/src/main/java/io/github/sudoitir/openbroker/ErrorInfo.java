package io.github.sudoitir.openbroker;

import java.util.Map;
import java.util.Objects;

/**
 * Why a call failed, in the AIP-193 style: a stable machine-readable {@code reason} within a {@code
 * domain}, and details in {@code metadata}. Standard reasons are in {@link ErrorReasons}; a driver
 * may add its own in its own domain. Metadata never holds a secret.
 */
public record ErrorInfo(String reason, String domain, Map<String, String> metadata) {

    public ErrorInfo {
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(domain, "domain");
        metadata = Map.copyOf(metadata);
    }

    public static ErrorInfo of(String reason) {
        return new ErrorInfo(reason, ErrorReasons.DOMAIN, Map.of());
    }

    public static ErrorInfo of(String reason, Map<String, String> metadata) {
        return new ErrorInfo(reason, ErrorReasons.DOMAIN, metadata);
    }
}
