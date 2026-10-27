package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** The observers of the connections: what each of the application's telemetry libraries contributes, combined. */
final class OpenBrokerObservers implements ObserverSource {

    private final List<ObserverContribution> contributions;
    private final Map<String, MessagingObserver> observers = new ConcurrentHashMap<>();

    OpenBrokerObservers(List<ObserverContribution> contributions) {
        this.contributions = List.copyOf(contributions);
    }

    @Override
    public MessagingObserver observer(String connection, String system) {
        return observers.computeIfAbsent(
                connection + '\0' + system,
                key -> MessagingObserver.composite(contributions.stream()
                        .map(contribution -> contribution.observer(connection, system))
                        .toArray(MessagingObserver[]::new)));
    }

    @Override
    public List<LinkListener> linkListeners(String connection, String system) {
        return contributions.stream()
                .map(contribution -> contribution.linkListener(connection, system))
                .filter(Objects::nonNull)
                .toList();
    }
}
