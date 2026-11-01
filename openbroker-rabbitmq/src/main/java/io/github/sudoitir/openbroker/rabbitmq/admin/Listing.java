package io.github.sudoitir.openbroker.rabbitmq.admin;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

/**
 * Paging over the management API: server paging for queues, exchanges, connections and channels ({@code page},
 * {@code page_size} at most 500, {@code name} with {@code use_regex}), client paging for the rest, and the opaque
 * continue token ({@code p:<page>} or {@code o:<offset>}, base64url).
 */
final class Listing {

    /** The largest page the management API is asked for. */
    static final int MAX_PAGE_SIZE = 500;

    /**
     * Where a continue token resumes: a server page number (from 1) or a client offset (from 0). Without a token
     * the start is page 1 and offset 0.
     */
    record Cursor(boolean server, int value) {

        /** The server page to ask for. */
        int page() {
            return server ? value : 1;
        }

        /** The client offset to resume at. */
        int offset() {
            return server ? 0 : value;
        }
    }

    private Listing() {}

    static String token(Cursor cursor) {
        String text = (cursor.server() ? "p:" : "o:") + cursor.value();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    /** The cursor a token names; the start when there is none. */
    static Cursor decode(Optional<String> token) {
        if (token.isEmpty()) {
            return new Cursor(false, 0);
        }
        try {
            String text = new String(Base64.getUrlDecoder().decode(token.get()), StandardCharsets.UTF_8);
            if (text.length() > 2 && text.charAt(1) == ':' && (text.charAt(0) == 'p' || text.charAt(0) == 'o')) {
                int value = Integer.parseInt(text.substring(2));
                boolean server = text.charAt(0) == 'p';
                if (value >= (server ? 1 : 0)) {
                    return new Cursor(server, value);
                }
            }
        } catch (IllegalArgumentException e) {
            // Falls through to the refusal below: a token that does not decode is not ours.
        }
        throw RabbitMqErrors.invalid("The continue token is not one this listing issued.");
    }
}
