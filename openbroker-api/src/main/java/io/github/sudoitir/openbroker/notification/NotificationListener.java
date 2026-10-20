package io.github.sudoitir.openbroker.notification;

/**
 * Receives notifications. The driver calls it serially per node, on a driver thread; a listener
 * that blocks delays only its own node's notifications, and past the driver's buffer it receives a
 * {@link Gap} instead of the dropped notifications.
 */
public interface NotificationListener {

    void onEvent(BrokerEvent event);

    void onGap(Gap gap);
}
