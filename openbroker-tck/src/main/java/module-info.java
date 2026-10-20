/** The OpenBroker driver conformance kit. */
module io.github.sudoitir.openbroker.tck {
    requires transitive io.github.sudoitir.openbroker;
    requires transitive org.junit.jupiter.api;
    requires org.assertj.core;
    requires static org.jspecify;

    exports io.github.sudoitir.openbroker.tck;

    opens io.github.sudoitir.openbroker.tck to
            org.junit.platform.commons;
}
