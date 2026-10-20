package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/** How the driver reads a query's paging, name pattern and ordering. */
public final class Queries {

    private Queries() {}

    /** The 1-based page a query asks for. */
    public static int page(ResourceQuery query) {
        return query.continueToken().map(Queries::parsePage).orElse(1);
    }

    /** A glob over the name, as a predicate. */
    public static Predicate<String> names(ResourceQuery query) {
        Optional<String> glob = query.namePattern();
        if (glob.isEmpty()) {
            return n -> true;
        }
        StringBuilder regex = new StringBuilder();
        for (char c : glob.get().toCharArray()) {
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append('.');
                default -> regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        Pattern p = Pattern.compile(regex.toString());
        return n -> p.matcher(n).matches();
    }

    /** Artemis lists in its own order; a query that asks for another is refused. */
    public static void requireNoOrdering(ResourceQuery query) {
        if (!query.orderBy().isEmpty()) {
            throw invalid("Artemis lists in its own order; this kind declares no ordering.");
        }
    }

    public static BrokerException invalid(String message) {
        return new BrokerException(
                Status.INVALID_ARGUMENT, ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT), Dispatch.NOT_SENT, null, message);
    }

    private static int parsePage(String token) {
        try {
            int page = Integer.parseInt(token);
            if (page < 1) {
                throw invalid("Not a continue token: " + token);
            }
            return page;
        } catch (NumberFormatException e) {
            throw invalid("Not a continue token: " + token);
        }
    }
}
