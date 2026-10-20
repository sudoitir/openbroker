/** OpenBroker: the consumer API and the driver SPI. */
module io.github.sudoitir.openbroker {
    requires static org.jspecify;

    exports io.github.sudoitir.openbroker;
    exports io.github.sudoitir.openbroker.access;
    exports io.github.sudoitir.openbroker.messaging;
    exports io.github.sudoitir.openbroker.model;
    exports io.github.sudoitir.openbroker.notification;
    exports io.github.sudoitir.openbroker.operation;
    exports io.github.sudoitir.openbroker.spi;

    uses io.github.sudoitir.openbroker.spi.BrokerDriver;
}
