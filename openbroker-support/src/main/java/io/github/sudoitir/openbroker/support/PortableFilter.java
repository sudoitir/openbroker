package io.github.sudoitir.openbroker.support;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * The {@value MessageFilter#PORTABLE} dialect, parsed once and evaluated client side, so a filter
 * works on a system that cannot filter on the server.
 *
 * <p>Grammar: {@code clause (AND clause)*}; {@code clause := field op 'text'}; {@code field :=
 * header.<name> | property.<name>}; {@code op := = | ^= | $=} (exact, prefix, suffix). A quote in
 * text is doubled. Header names are those of {@link MessageHeaders}. A clause on a missing field
 * does not match.
 */
public final class PortableFilter implements Predicate<Message> {

    private record Clause(boolean header, String name, String op, String text) {}

    private final List<Clause> clauses;

    private PortableFilter(List<Clause> clauses) {
        this.clauses = List.copyOf(clauses);
    }

    /**
     * Parses a portable filter.
     *
     * @throws BrokerException with {@code INVALID_ARGUMENT} and reason {@code INVALID_FILTER} when
     *     the dialect is not portable or the expression does not parse
     */
    public static PortableFilter parse(MessageFilter filter) {
        if (!filter.dialect().equals(MessageFilter.PORTABLE)) {
            throw invalid("Not the portable dialect: " + filter.dialect());
        }
        return new PortableFilter(new Parser(filter.expression()).clauses());
    }

    @Override
    public boolean test(Message message) {
        for (Clause c : clauses) {
            Optional<String> value = c.header() ? header(message.headers(), c.name()) : property(message, c.name());
            if (value.isEmpty() || !matches(value.get(), c.op(), c.text())) {
                return false;
            }
        }
        return true;
    }

    private static boolean matches(String value, String op, String text) {
        return switch (op) {
            case "=" -> value.equals(text);
            case "^=" -> value.startsWith(text);
            case "$=" -> value.endsWith(text);
            default -> throw new IllegalStateException(op);
        };
    }

    private static Optional<String> property(Message message, String name) {
        Map<String, PropertyValue> properties = message.properties();
        return Optional.ofNullable(properties.get(name)).map(PropertyValue::asText);
    }

    private static Optional<String> header(MessageHeaders h, String name) {
        return switch (name) {
            case "id" -> h.id();
            case "correlationId" -> h.correlationId();
            case "replyTo" -> h.replyTo();
            case "contentType" -> h.contentType();
            case "type" -> h.type();
            case "groupId" -> h.groupId();
            case "deduplicationId" -> h.deduplicationId();
            case "priority" ->
                h.priority().isPresent()
                        ? Optional.of(String.valueOf(h.priority().getAsInt()))
                        : Optional.empty();
            case "durable" -> h.durable().map(String::valueOf);
            default -> throw invalid("Unknown header in filter: " + name);
        };
    }

    private static BrokerException invalid(String message) {
        return new BrokerException(
                Status.INVALID_ARGUMENT, ErrorInfo.of(ErrorReasons.INVALID_FILTER), Dispatch.NOT_SENT, null, message);
    }

    private static final class Parser {
        private final String s;
        private int i;

        Parser(String s) {
            this.s = s;
        }

        List<Clause> clauses() {
            List<Clause> out = new ArrayList<>();
            skipSpaces();
            if (i == s.length()) {
                throw invalid("An empty filter");
            }
            out.add(clause());
            skipSpaces();
            while (i < s.length()) {
                if (!s.startsWith("AND", i) || i + 3 >= s.length() || !Character.isWhitespace(s.charAt(i + 3))) {
                    throw invalid("Expected AND at " + i + " in: " + s);
                }
                i += 3;
                skipSpaces();
                out.add(clause());
                skipSpaces();
            }
            return out;
        }

        private Clause clause() {
            boolean header;
            if (s.startsWith("header.", i)) {
                header = true;
                i += 7;
            } else if (s.startsWith("property.", i)) {
                header = false;
                i += 9;
            } else {
                throw invalid("A clause starts with header. or property. at " + i + " in: " + s);
            }
            int start = i;
            while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || "_-.$".indexOf(s.charAt(i)) >= 0)) {
                i++;
            }
            if (i == start) {
                throw invalid("A field name is missing at " + i + " in: " + s);
            }
            String name = s.substring(start, i);
            skipSpaces();
            String op;
            if (s.startsWith("^=", i) || s.startsWith("$=", i)) {
                op = s.substring(i, i + 2);
                i += 2;
            } else if (s.startsWith("=", i)) {
                op = "=";
                i++;
            } else {
                throw invalid("Expected =, ^= or $= at " + i + " in: " + s);
            }
            skipSpaces();
            return new Clause(header, name, op, text());
        }

        private String text() {
            if (i >= s.length() || s.charAt(i) != '\'') {
                throw invalid("Expected a quoted text at " + i + " in: " + s);
            }
            i++;
            StringBuilder out = new StringBuilder();
            while (true) {
                if (i >= s.length()) {
                    throw invalid("An unterminated text in: " + s);
                }
                char c = s.charAt(i++);
                if (c == '\'') {
                    if (i < s.length() && s.charAt(i) == '\'') {
                        out.append('\'');
                        i++;
                    } else {
                        return out.toString();
                    }
                } else {
                    out.append(c);
                }
            }
        }

        private void skipSpaces() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }
    }
}
