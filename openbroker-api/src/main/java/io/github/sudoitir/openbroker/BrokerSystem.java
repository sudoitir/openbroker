package io.github.sudoitir.openbroker;

import java.util.Objects;
import java.util.Set;

/**
 * A broker or queue system a driver supports.
 *
 * @param id the stable identifier ({@code artemis}, {@code kafka}); it prefixes the driver's kinds
 * @param name the system's display name
 * @param supportedVersions the version range the driver is tested against, in Maven range syntax
 * @param schemaVersion the version of the driver's {@link ConnectionSchema}
 * @param nativePackages the Java packages of the system's own client libraries; a host that runs
 *     untrusted code can deny them, so only the driver talks to the broker
 */
public record BrokerSystem(
        String id, String name, String supportedVersions, int schemaVersion, Set<String> nativePackages) {

    public BrokerSystem {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(supportedVersions, "supportedVersions");
        nativePackages = Set.copyOf(nativePackages);
    }
}
