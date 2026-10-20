package io.github.sudoitir.openbroker.artemis.internal;

import java.util.Map;

/**
 * The JMS-defined properties a client sets and reads by their JMS names, which Artemis stores under
 * its own. They are read and written by the JMS names, as a JMS client would, so a message keeps
 * them when it is read and sent again.
 */
final class JmsNames {

    /** JMS name to the name Artemis stores the property under. */
    static final Map<String, String> STORED =
            Map.of("JMSXGroupID", "_AMQ_GROUP_ID", "JMSXGroupSeq", "_AMQ_GROUP_SEQUENCE");

    /** Artemis's stored name to the JMS name. */
    static final Map<String, String> JMS = Map.of("_AMQ_GROUP_SEQUENCE", "JMSXGroupSeq");

    private JmsNames() {}

    /** The name a property is stored under. */
    static String stored(String name) {
        return STORED.getOrDefault(name, name);
    }

    /** The name a stored property is read by. */
    static String read(String name) {
        return JMS.getOrDefault(name, name);
    }
}
