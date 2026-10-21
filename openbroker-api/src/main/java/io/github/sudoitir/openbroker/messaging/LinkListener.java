package io.github.sudoitir.openbroker.messaging;

/** Hears link state changes. Events of one link arrive serially, in order. */
@FunctionalInterface
public interface LinkListener {

    void changed(LinkEvent event);
}
