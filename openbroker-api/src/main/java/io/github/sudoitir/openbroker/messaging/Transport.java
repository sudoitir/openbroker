package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.Incubating;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;

/**
 * Long-lived receivers and senders, for moving messages reliably. A transacted batch is the
 * capability {@code transport.transacted}; asking for one without it fails with {@code
 * UNIMPLEMENTED}.
 */
@Incubating
public interface Transport {

    /** A receiver on one node of the subscription; a scope of more than one node is refused. */
    Receiver receiver(ResourceId subscription, Scope scope, ReceiveOptions options);

    /** A sender entering through the first node of {@code scope} that holds the target. */
    Sender sender(SendTarget target, Scope scope, SenderOptions options);
}
