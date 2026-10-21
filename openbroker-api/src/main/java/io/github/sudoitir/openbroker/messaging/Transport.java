package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;

/**
 * Long-lived senders, receivers and processors, for moving messages reliably. Opening a link outside
 * {@link #capabilities()} fails with {@code UNIMPLEMENTED}.
 */
public interface Transport {

    /** What this transport can do; static for the connection. */
    TransportCapabilities capabilities();

    /** A sender entering through the first node of {@code scope} that holds the target. */
    Sender sender(SendTarget target, Scope scope, SenderOptions options);

    /** A receiver on one node of the subscription; a scope of more than one node is refused. */
    Receiver receiver(ResourceId subscription, Scope scope, ReceiverOptions options);

    /** Runs {@code handler} on each message of the subscription, in lanes, until the processor is closed. */
    Processor process(ResourceId subscription, Scope scope, ProcessorOptions options, MessageHandler handler);

    /** Hears the state changes of every link of this transport until closed. */
    AutoCloseable watch(LinkListener listener);
}
