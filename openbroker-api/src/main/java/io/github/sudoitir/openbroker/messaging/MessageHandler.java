package io.github.sudoitir.openbroker.messaging;

/** Handles one message and says how to settle it. A thrown exception counts as a failed attempt. */
@FunctionalInterface
public interface MessageHandler {

    Disposition handle(MessageContext context) throws Exception;
}
