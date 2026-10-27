package io.github.sudoitir.openbroker.kafka.admin;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.TreeMap;

/** Etags: a fingerprint of the state an operation acts on, so a run refuses a state that changed since its preview. */
final class Etags {

    private Etags() {}

    /** The etag of the given parts, in order. Map parts are sorted by key. */
    static String of(Object... parts) {
        StringBuilder text = new StringBuilder();
        for (Object part : parts) {
            if (part instanceof Map<?, ?> map) {
                new TreeMap<>(map)
                        .forEach((k, v) -> text.append(k).append('=').append(v).append(';'));
            } else {
                text.append(part);
            }
            text.append('\u0000');
        }
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256").digest(text.toString().getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Every Java runtime has SHA-256", e);
        }
    }
}
