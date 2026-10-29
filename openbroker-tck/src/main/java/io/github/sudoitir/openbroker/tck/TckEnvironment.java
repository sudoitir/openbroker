package io.github.sudoitir.openbroker.tck;

import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import java.util.Optional;
import java.util.Set;

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
     * Every kind of subscription this environment can make, which the cases that run per kind cover.
     * The default is {@link #subscriptionKind()} alone.
     */
    default Set<KindId> subscriptionKinds() {
        return Set.of(subscriptionKind());
    }

    /** The id a new subscription of {@code kind} named {@code name} gets. */
    default ResourceId subscriptionId(KindId kind, String name) {
        requireOwnKind(kind);
        return subscriptionId(name);
    }

    /** The attributes that create a durable standalone subscription of {@code kind} named {@code name}. */
    default Attributes subscriptionAttributes(KindId kind, String name) {
        requireOwnKind(kind);
        return subscriptionAttributes(name);
    }

    /**
     * Whether receivers of one subscription compete for its messages, as consumers of a queue do. False
     * where each receiver owns a share of the messages (a partition), so a message one receiver holds
     * stays with it. The default is true.
     */
    default boolean competingReceivers() {
        return true;
    }

    private void requireOwnKind(KindId kind) {
        if (!kind.equals(subscriptionKind())) {
            throw new IllegalArgumentException("This environment makes only " + subscriptionKind() + ", not " + kind);
        }
    }

    /**
     * A valid filter in the driver's own dialect matching messages whose string property {@code
     * name} equals {@code value}.
     */
    /**
     * The shard that holds the messages sent to {@code subscription}, which a browse is scoped to and
     * names offsets of. A system whose copies are on nodes names the first node of the request; one whose
     * log is in partitions names the partition.
     */
    default Shard browseShard(ResourceId subscription) {
        return Shard.node(request().cluster().nodes().getFirst().id());
    }

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
     * A request whose only address is {@code host:port}, with the account of {@link #request()}. The
     * address is one nothing answers on, so the case that classifies an unreachable address can use it.
     * Empty (the default) when the driver cannot give one; the case is then skipped.
     */
    default Optional<ConnectRequest> requestToAddress(String host, int port) {
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
