package io.github.sudoitir.openbroker.artemis;

import io.github.sudoitir.openbroker.access.Remedy;

/** The {@code broker.xml} changes that make an Artemis capability available. */
public final class ArtemisRemedies {

    private static final String XML = "application/xml";

    /**
     * Artemis matches the single most specific {@code security-setting}, so this block restates every
     * permission a subscriber needs on {@code activemq.notifications}; {@code consume} alone fails
     * with {@code AMQ229213 ... CREATE_NON_DURABLE_QUEUE}.
     */
    public static final Remedy NOTIFICATIONS_SECURITY = new Remedy(
            "Grant the account's role consume, createNonDurableQueue and deleteNonDurableQueue on"
                    + " activemq.notifications.",
            """
            <security-setting match="activemq.notifications">
              <permission type="consume" roles="amq"/>
              <permission type="createNonDurableQueue" roles="amq"/>
              <permission type="deleteNonDurableQueue" roles="amq"/>
            </security-setting>
            """,
            XML);

    /**
     * Without this plugin the connection, session, delivered and expired notifications are never
     * emitted at all; no permission makes them appear.
     */
    public static final Remedy NOTIFICATION_PLUGIN = new Remedy(
            "Enable the notification plugin for connection, session, delivered and expired events.", """
            <broker-plugins>
              <broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.NotificationActiveMQServerPlugin">
                <property key="SEND_CONNECTION_NOTIFICATIONS" value="true"/>
                <property key="SEND_SESSION_NOTIFICATIONS" value="true"/>
                <property key="SEND_DELIVERED_NOTIFICATIONS" value="true"/>
                <property key="SEND_EXPIRED_NOTIFICATIONS" value="true"/>
              </broker-plugin>
            </broker-plugins>
            """, XML);

    /**
     * Management writes pass two gates: the broker's {@code manage} permission on {@code
     * activemq.management}, and the console in front of Jolokia, which refuses with a bare 403 unless
     * the user holds a role it admits.
     */
    public static final Remedy MANAGEMENT_SECURITY = new Remedy(
            "Grant the account's role manage on activemq.management.#, and a role the console admits.", """
            <!-- 1. The broker-side permission the management operations check. -->
            <security-setting match="activemq.management.#">
              <permission type="createNonDurableQueue" roles="amq"/>
              <permission type="createAddress"         roles="amq"/>
              <permission type="consume"               roles="amq"/>
              <permission type="send"                  roles="amq"/>
              <permission type="manage"                roles="amq"/>
            </security-setting>

            <!-- 2. The console/Jolokia layer refuses with HTTP 403 before the broker
                 is reached, so the management user must hold a role it admits. Grant
                 it in etc/artemis-roles.properties, e.g.:
                   amq = <your-management-user>
            -->
            """, XML);

    /** A CORE acceptor, for notifications and faithful message I/O. */
    public static final Remedy CORE_ACCEPTOR =
            new Remedy("Expose a CORE acceptor and give each node a Core endpoint the client can reach.", """
            <acceptors>
              <acceptor name="artemis">tcp://0.0.0.0:61616?protocols=CORE,AMQP,STOMP,MQTT,OPENWIRE</acceptor>
            </acceptors>
            """, XML);

    /**
     * A managed retry counts attempts in the client, but the broker counts too: it dead-letters a
     * message after the address's {@code max-delivery-attempts}, so a policy allowing more attempts
     * than that never reaches its last one.
     */
    public static Remedy maxDeliveryAttempts(String address, int attempts) {
        return new Remedy(
                "Let the broker deliver a message of " + address + " at least " + attempts
                        + " times before it dead-letters, or -1 for no limit.",
                """
                <address-setting match="%s">
                  <max-delivery-attempts>-1</max-delivery-attempts>
                </address-setting>
                """.formatted(address),
                XML);
    }

    private ArtemisRemedies() {}
}
