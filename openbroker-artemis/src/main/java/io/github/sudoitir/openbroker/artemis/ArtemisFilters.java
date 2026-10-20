package io.github.sudoitir.openbroker.artemis;

import io.github.sudoitir.openbroker.messaging.MessageFilter;

/** The Artemis filter dialect: the broker's selector grammar, the SQL-92 subset JMS selectors use. */
public final class ArtemisFilters {

    /** The dialect name of an Artemis selector. */
    public static final String DIALECT = "artemis.selector";

    private ArtemisFilters() {}

    public static MessageFilter selector(String expression) {
        return new MessageFilter(DIALECT, expression);
    }
}
