package io.github.sudoitir.openbroker.kafka.schema;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/** An etag over a subject's name, its versions and its compatibility. Hex of the first 16 digest bytes. */
final class SchemaEtags {

    private SchemaEtags() {}

    /**
     * The deleted bit is part of each version. Without it a soft delete would not change the etag, and a preview taken
     * before the delete would still match.
     */
    static String of(String subject, List<SubjectVersion> versions, String compatibility) {
        StringBuilder text = new StringBuilder();
        text.append(subject).append('\u0000');
        for (SubjectVersion version : versions) {
            text.append(version.id())
                    .append('\u0000')
                    .append(version.version())
                    .append('\u0000')
                    .append(version.deleted() ? '1' : '0')
                    .append('\u0000');
        }
        text.append(compatibility).append('\u0000');
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256").digest(text.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Every Java runtime has SHA-256", e);
        }
    }
}
