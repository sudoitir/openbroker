package io.github.sudoitir.openbroker.artemis;

/**
 * Artemis JMX object names. The broker's {@code <name>} is not known up front, so it is resolved
 * with {@link #BROKER_SEARCH_PATTERN}, which matches only the top-level broker MBean.
 */
public final class ArtemisMBeans {

    public static final String DOMAIN = "org.apache.activemq.artemis";
    public static final String BROKER_SEARCH_PATTERN = DOMAIN + ":broker=*";

    private ArtemisMBeans() {}

    public static String acceptorsPattern(String broker) {
        return broker + ",component=acceptors,name=*";
    }

    public static String addressesPattern(String broker) {
        return broker + ",component=addresses,address=*";
    }

    public static String address(String broker, String address) {
        return broker + ",component=addresses,address=" + quote(address);
    }

    public static String queue(String broker, String address, String queue, String routingType) {
        return broker + ",component=addresses,address=" + quote(address)
                + ",subcomponent=queues,routing-type=" + quote(routingType.toLowerCase())
                + ",queue=" + quote(queue);
    }

    /** Every MBean of the queue with this name, whatever its address and routing type. */
    public static String queuePattern(String broker, String queue) {
        return broker + ",component=addresses,address=*,subcomponent=queues,routing-type=*,queue=" + quote(queue);
    }

    public static String divertsPattern(String broker) {
        return broker + ",component=addresses,address=*,subcomponent=diverts,divert=*";
    }

    public static String bridgesPattern(String broker) {
        return broker + ",component=bridges,name=*";
    }

    private static String quote(String value) {
        return '"' + value + '"';
    }
}
