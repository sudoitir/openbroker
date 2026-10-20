package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;

/**
 * A filter in a named dialect. A driver declares its own dialect ({@code artemis.core-filter}) and
 * parses an expression before use, refusing one that is not a single complete expression with
 * {@code INVALID_FILTER}. The portable dialect {@value #PORTABLE} works on every system.
 *
 * <p>The portable grammar is clauses joined by {@code AND}; a clause is {@code <field> <op>
 * '<text>'}, where the field is {@code header.<name>} or {@code property.<name>}, the operator is
 * {@code =} (exact), {@code ^=} (prefix) or {@code $=} (suffix), and a quote inside text is doubled.
 */
public record MessageFilter(String dialect, String expression) {

    public static final String PORTABLE = "openbroker.match";

    public MessageFilter {
        Objects.requireNonNull(dialect, "dialect");
        Objects.requireNonNull(expression, "expression");
    }

    public static MessageFilter portable(String expression) {
        return new MessageFilter(PORTABLE, expression);
    }
}
