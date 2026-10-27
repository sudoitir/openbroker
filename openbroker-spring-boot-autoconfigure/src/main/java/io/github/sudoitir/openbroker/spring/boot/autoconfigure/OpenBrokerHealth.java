package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import java.util.Collection;
import java.util.stream.Stream;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthContributors;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

/** The {@code openbroker} health: one indicator per connection, out of service once the context closes. */
final class OpenBrokerHealth implements CompositeHealthContributor, ApplicationListener<ContextClosedEvent> {

    private final CompositeHealthContributor delegate;
    private final Collection<ConnectionHealthIndicator> indicators;

    OpenBrokerHealth(CompositeHealthContributor delegate, Collection<ConnectionHealthIndicator> indicators) {
        this.delegate = delegate;
        this.indicators = indicators;
    }

    @Override
    public HealthContributor getContributor(String name) {
        return delegate.getContributor(name);
    }

    @Override
    public Stream<HealthContributors.Entry> stream() {
        return delegate.stream();
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        indicators.forEach(ConnectionHealthIndicator::closing);
    }
}
