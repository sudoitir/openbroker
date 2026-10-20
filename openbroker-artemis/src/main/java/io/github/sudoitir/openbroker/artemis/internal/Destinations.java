package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.model.ResourceId;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/** A queue's Core name: its fully qualified {@code address::queue}, so a call reaches this queue only. */
final class Destinations {

    private Destinations() {}

    static String of(Cluster cluster, NodeHandle node, ResourceId queue, Deadline deadline) {
        if (node.jolokia().isEmpty()) {
            // Without management the address cannot be read; a queue created on its own address is named so.
            return queue.name() + "::" + queue.name();
        }
        String mbean = cluster.queueMBean(node, queue.name(), deadline);
        try {
            return ObjectName.unquote(ObjectName.getInstance(mbean).getKeyProperty("address")) + "::" + queue.name();
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException("Not a queue MBean: " + mbean, e);
        }
    }
}
