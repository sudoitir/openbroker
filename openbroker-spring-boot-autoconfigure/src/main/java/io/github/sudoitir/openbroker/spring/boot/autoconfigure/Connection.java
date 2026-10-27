package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * One connection under {@code openbroker.connections.<name>}: where the cluster is, how to sign in and
 * how its clients behave. The SDK's own records bind here, each over its own defaults.
 *
 * @param system the broker system, such as {@code kafka} or {@code artemis}
 * @param cluster the nodes and endpoints, and the driver's options
 * @param credentials the credentials for every plane
 * @param managementCredentials the credentials for the management plane, if they differ
 * @param options what configuration can say of the connect options
 * @param sender the defaults for senders on this connection
 * @param receiver the defaults for receivers on this connection
 * @param processor the defaults for processors on this connection
 */
public record Connection(
        @Nullable String system,
        ClusterSpec cluster,
        CredentialsProperties credentials,
        CredentialsProperties managementCredentials,
        ConnectionOptions options,
        SenderOptions sender,
        ReceiverOptions receiver,
        ProcessorOptions processor) {

    /** A connection with nothing set: every part takes the SDK's default. */
    public static Connection defaults() {
        return new Connection(
                null,
                new ClusterSpec(List.of(), List.of(), Map.of()),
                CredentialsProperties.unset(),
                CredentialsProperties.unset(),
                ConnectionOptions.unset(),
                SenderOptions.defaults(),
                ReceiverOptions.consume(),
                ProcessorOptions.defaults());
    }
}
