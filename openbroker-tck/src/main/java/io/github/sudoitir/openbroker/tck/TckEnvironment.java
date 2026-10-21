package io.github.sudoitir.openbroker.tck;

import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import java.util.Optional;

/**
 * What a driver gives the TCK: itself, a request that reaches a real test cluster, and how to make
 * the resources the cases need in its system.
 */
public interface TckEnvironment {

    BrokerDriver driver();

    /** Reaches the test cluster with an account that may read, write, send and receive. */
    ConnectRequest request();

    /** The same cluster, with a wrong password for the same account. */
    ConnectRequest requestWithWrongPassword(String wrongPassword);

    /** The kind of a standalone subscription, which messages can be sent to directly. */
    KindId subscriptionKind();

    /** The id a new subscription named {@code name} gets. */
    ResourceId subscriptionId(String name);

    /** The attributes that create a durable standalone subscription named {@code name}. */
    Attributes subscriptionAttributes(String name);

    /**
     * A valid filter in the driver's own dialect matching messages whose string property {@code
     * name} equals {@code value}.
     */
    MessageFilter nativeEquals(String name, String value);

    /** The driver's own dialect, to test that an escape from a composed filter is refused. */
    String nativeDialect();

    /**
     * The same cluster, with an account that may send to and receive from subscriptions named {@code
     * tck.*}, except those named {@code tck.denied.*}, which it may neither send to nor receive from.
     * Empty (the default) when the driver cannot give such an account; the authorization cases are
     * then skipped.
     */
    default Optional<ConnectRequest> restrictedRequest() {
        return Optional.empty();
    }

    /**
     * A request that reaches the cluster through an endpoint that requires a client certificate, and
     * authenticates with one. Empty (the default) when the driver cannot give one; the mutual TLS
     * case is then skipped.
     */
    default Optional<ConnectRequest> mutualTlsRequest() {
        return Optional.empty();
    }
}
