package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import org.jspecify.annotations.Nullable;

/** What one of the application's telemetry libraries adds to a connection's observer. */
interface ObserverContribution {

    MessagingObserver observer(String connection, String system);

    /** The same library's listener of link events, if it has one. */
    @Nullable LinkListener linkListener(String connection, String system);
}
