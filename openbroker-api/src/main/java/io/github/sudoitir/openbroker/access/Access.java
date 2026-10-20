package io.github.sudoitir.openbroker.access;

import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.Optional;

/**
 * What this account may do here, checked live, like Kubernetes {@code SelfSubjectAccessReview}. The
 * static half, what the system can do at all, is each kind's verbs.
 */
public interface Access {

    Assessment check(Capability capability, Optional<ResourceId> resource);

    /** Assesses every capability the driver knows. */
    AccessSnapshot probe();
}
