package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.artemis.ArtemisFilters;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.support.PortableFilter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.activemq.artemis.selector.filter.FilterException;
import org.apache.activemq.artemis.selector.impl.SelectorParser;

/**
 * Artemis selectors, parsed with the broker's own grammar before they are sent or composed. A filter
 * must be one complete expression: {@code a=1) OR (1=1} closes a parenthesis itself, so composing it
 * into {@code (<filter>) AND <bound>} would escape the bound.
 */
public final class Selectors {

    private Selectors() {}

    /** The selector of a native filter, validated; empty for no filter. */
    public static String of(Optional<MessageFilter> filter) {
        return filter.map(Selectors::of).orElse("");
    }

    public static String of(MessageFilter filter) {
        if (filter.dialect().equals(MessageFilter.PORTABLE)) {
            return translate(PortableFilter.parse(filter));
        }
        if (!filter.dialect().equals(ArtemisFilters.DIALECT)) {
            throw invalid("Artemis filters in its own dialect, " + ArtemisFilters.DIALECT + ", or the portable one.");
        }
        String expression = filter.expression().strip();
        if (!expression.isEmpty()) {
            requireComplete(expression);
        }
        return expression;
    }

    /** A portable filter as the selector that matches the same messages. */
    static String translate(PortableFilter filter) {
        List<String> terms = new ArrayList<>();
        for (PortableFilter.Clause c : filter.clauses()) {
            String field = c.header() ? header(c.name()) : property(c.name());
            String text = c.text();
            terms.add(
                    switch (c.op()) {
                        case "=" -> field + " = " + literal(text);
                        case "^=" -> field + " LIKE " + literal(like(text) + "%") + " ESCAPE '!'";
                        case "$=" -> field + " LIKE " + literal("%" + like(text)) + " ESCAPE '!'";
                        default -> throw invalid("Unknown operator " + c.op() + ".");
                    });
        }
        return String.join(" AND ", terms);
    }

    /** The selector identifier of a header Artemis can filter on. */
    private static String header(String name) {
        return switch (name) {
            case "id" -> "JMSMessageID";
            case "correlationId" -> "JMSCorrelationID";
            case "type" -> "JMSType";
            case "priority" -> "JMSPriority";
            case "durable" -> "JMSDeliveryMode";
            case "groupId" -> "JMSXGroupID";
            case "deduplicationId" -> "_AMQ_DUPL_ID";
            default -> throw invalid("Artemis cannot filter on the header " + name + ".");
        };
    }

    private static String property(String name) {
        if (name.isEmpty()
                || !Character.isJavaIdentifierStart(name.charAt(0))
                || !name.chars().skip(1).allMatch(Character::isJavaIdentifierPart)) {
            throw invalid("Artemis filters only on properties named like identifiers.");
        }
        return name;
    }

    private static String literal(String text) {
        return "'" + text.replace("'", "''") + "'";
    }

    private static String like(String text) {
        return text.replace("!", "!!").replace("%", "!%").replace("_", "!_");
    }

    /** The filter bounded to messages produced at or before {@code bound}, when there is one. */
    public static String bounded(String selector, Optional<Instant> bound) {
        if (bound.isEmpty()) {
            return selector;
        }
        String frozen = "AMQTimestamp <= " + bound.get().toEpochMilli();
        return selector.isEmpty() ? frozen : "(" + selector + ") AND " + frozen;
    }

    private static void requireComplete(String expression) {
        try {
            SelectorParser.parse(expression);
        } catch (FilterException | RuntimeException e) {
            // Never echo the expression: a filter can carry sensitive literals.
            throw invalid("Invalid message filter.");
        }
    }

    static BrokerException invalid(String message) {
        return new BrokerException(
                Status.INVALID_ARGUMENT, ErrorInfo.of(ErrorReasons.INVALID_FILTER), Dispatch.NOT_SENT, null, message);
    }
}
