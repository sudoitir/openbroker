package io.github.sudoitir.openbroker.messaging;

/** What a handler is given: the message and the attempt it is on. */
public interface MessageContext {

    Message message();

    /** The 1-based attempt: the system's delivery count, plus in-place retries. */
    int attempt();

    /** A sender joined to this delivery's transaction; {@code TRANSACTED} processors only. */
    Sender sender(SendTarget target);
}
