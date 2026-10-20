package io.github.sudoitir.openbroker.artemis;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.artemis.internal.Selectors;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import java.time.Instant;
import java.util.Optional;

/** The Artemis filter dialect: the broker's selector grammar, the SQL-92 subset JMS selectors use. */
public final class ArtemisFilters {

    /** The dialect name of an Artemis selector. */
    public static final String DIALECT = "artemis.selector";

    private ArtemisFilters() {}

    public static MessageFilter selector(String expression) {
        return new MessageFilter(DIALECT, expression);
    }

    /**
     * A selector matching what {@code filter} matches among the messages produced at or before {@code
     * bound}, for a management operation that takes a selector and no bound. The filter must be one
     * complete expression, so that it cannot escape the bound.
     *
     * @throws BrokerException with reason {@code INVALID_FILTER} when it
     *     is not
     */
    public static MessageFilter producedAtOrBefore(Optional<MessageFilter> filter, Instant bound) {
        return selector(Selectors.bounded(Selectors.of(filter), Optional.of(bound)));
    }
}
