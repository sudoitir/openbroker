package io.github.sudoitir.openbroker.rabbitmq.admin;

import java.util.Optional;

/**
 * Paging over the management API: server paging for queues, exchanges, connections and channels ({@code page},
 * {@code page_size} at most 500, {@code name} with {@code use_regex}), client paging for the rest, and the opaque
 * continue token ({@code p:<page>} or {@code o:<offset>}, base64url).
 */
final class Listing {

    /** The largest page the management API is asked for. */
    static final int MAX_PAGE_SIZE = 500;

    /** Where a continue token resumes: a server page number or a client offset. */
    record Cursor(boolean server, int value) {}

    private Listing() {}

    static String token(Cursor cursor) {
        throw RabbitMqErrors.unimplemented("Listing.token");
    }

    static Cursor decode(Optional<String> token) {
        throw RabbitMqErrors.unimplemented("Listing.decode");
    }
}
