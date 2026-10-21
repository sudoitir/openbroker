package io.github.sudoitir.openbroker.messaging;

import java.util.List;
import org.jspecify.annotations.Nullable;

/** One observed operation, from its start to {@link #end}. */
public interface Observation {

    /** Makes the operation current around a handler; close it when the handler returns. */
    default AutoCloseable scope() {
        return () -> {};
    }

    /** The messages a pull receive returned, to link to. */
    default void received(List<Message> messages) {}

    /** The operation finished: a receipt completed, a handler ended or a settlement ended. */
    void end(@Nullable Throwable error);
}
