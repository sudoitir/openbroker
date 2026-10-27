package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import java.util.UUID;

/** Secret-looking values for tests, made fresh for each run so that no credential is written into the source. */
final class TestSecrets {

    static final String PASSWORD = UUID.randomUUID().toString();
    static final String OTHER_PASSWORD = UUID.randomUUID().toString();
    static final String TOKEN = UUID.randomUUID().toString();
    /** A value that is wrong for the key it is given, and must appear in no message. */
    static final String BAD = UUID.randomUUID().toString();

    private TestSecrets() {}
}
