package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.Objects;

/** What a bulk action does to each selected message. */
public sealed interface MessageAction {

    record Move(ResourceId target) implements MessageAction {
        public Move {
            Objects.requireNonNull(target, "target");
        }
    }

    record Copy(ResourceId target) implements MessageAction {
        public Copy {
            Objects.requireNonNull(target, "target");
        }
    }

    record Delete() implements MessageAction {}

    /** Sends a dead-lettered message back to where it came from. */
    record Retry() implements MessageAction {}

    /** Sends the message to its dead-letter destination. */
    record DeadLetter() implements MessageAction {}
}
