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

    Receiver receiver(ResourceId subscription, Scope scope, ReceiveOptions options);

    Sender sender(SendTarget target, SenderOptions options);
}
