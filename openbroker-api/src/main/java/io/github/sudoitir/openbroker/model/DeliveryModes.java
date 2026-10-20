package io.github.sudoitir.openbroker.model;

import java.util.EnumSet;
import java.util.Set;

/** How a destination or subscription delivers: one way, or both where the system allows it. */
public record DeliveryModes(Set<Delivery> modes) {

    public DeliveryModes {
        modes = Set.copyOf(modes);
    }

    public static DeliveryModes of(Delivery first, Delivery... rest) {
        return new DeliveryModes(EnumSet.of(first, rest));
    }

    public boolean includes(Delivery delivery) {
        return modes.contains(delivery);
    }
}
