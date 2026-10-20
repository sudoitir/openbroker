package io.github.sudoitir.openbroker.messaging;

import java.util.Optional;

/**
 * The {@value #NAMESPACE} extension of a message, which a driver for a JMS system fills: what a JMS
 * client would see that has no portable header.
 */
public final class JmsExtension {

    public static final String NAMESPACE = "jms";

    /** The JMS message type, as an int: see the constants below. */
    public static final String MESSAGE_TYPE = "jms.message_type";

    public static final int GENERIC = 0;
    public static final int OBJECT = 2;
    public static final int TEXT = 3;
    public static final int BYTES = 4;
    public static final int MAP = 5;
    public static final int STREAM = 6;

    private JmsExtension() {}

    /** The message's JMS type, when its system says. */
    public static Optional<Integer> messageType(Message message) {
        return message.extension(NAMESPACE)
                .raw(MESSAGE_TYPE)
                .filter(Number.class::isInstance)
                .map(n -> ((Number) n).intValue());
    }
}
