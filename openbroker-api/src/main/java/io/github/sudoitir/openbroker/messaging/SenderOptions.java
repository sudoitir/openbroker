package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.Incubating;

/** How a sender sends. */
@Incubating
public record SenderOptions(boolean transacted) {}
