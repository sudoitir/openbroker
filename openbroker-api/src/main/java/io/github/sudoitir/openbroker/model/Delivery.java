package io.github.sudoitir.openbroker.model;

/** How a destination delivers, in the EIP and JMS names. A destination may do both. */
public enum Delivery {
    /** Each message goes to one consumer: a queue. */
    POINT_TO_POINT,
    /** Each message goes to every subscription: a topic. */
    PUBLISH_SUBSCRIBE
}
