/** OpenBroker OpenTelemetry: traces and metrics for messaging. */
module io.github.sudoitir.openbroker.opentelemetry {
    requires transitive io.github.sudoitir.openbroker;
    requires transitive io.opentelemetry.api;
    requires io.opentelemetry.context;
    requires static org.jspecify;

    exports io.github.sudoitir.openbroker.opentelemetry;
}
