package io.github.sudoitir.openbroker.artemis;

import io.github.sudoitir.openbroker.model.KindId;

/** The kinds the Artemis driver defines. */
public final class ArtemisKinds {

    /** An address: a destination, point-to-point, publish-subscribe or both. */
    public static final KindId ADDRESS = KindId.of("artemis.address");
    /** A queue: a subscription, bound to one address. */
    public static final KindId QUEUE = KindId.of("artemis.queue");
    /** A divert: a route that forwards from one address to another. */
    public static final KindId DIVERT = KindId.of("artemis.divert");
    /** A core bridge: a route that links a queue to an address on another broker. */
    public static final KindId BRIDGE = KindId.of("artemis.bridge");

    public static final KindId CONNECTION = KindId.of("artemis.connection");
    /** A session: the channel class. */
    public static final KindId SESSION = KindId.of("artemis.session");

    public static final KindId CONSUMER = KindId.of("artemis.consumer");
    public static final KindId PRODUCER = KindId.of("artemis.producer");
    /** Address settings matched by an address pattern: a policy. */
    public static final KindId ADDRESS_SETTING = KindId.of("artemis.address-setting");
    /** Security settings matched by an address pattern: an access rule. */
    public static final KindId SECURITY_SETTING = KindId.of("artemis.security-setting");

    public static final KindId USER = KindId.of("artemis.user");
    public static final KindId TRANSACTION = KindId.of("artemis.transaction");

    /** Native attribute keys. */
    public static final String ROUTING_TYPE = "artemis.routing_type";

    private ArtemisKinds() {}
}
