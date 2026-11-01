package io.github.sudoitir.openbroker.operation;

/** What a {@link Preview}'s etag holds, and so how strongly it identifies the resource it was taken from. */
public enum EtagBasis {
    /**
     * The etag holds an identity the system assigned to the resource, so a resource deleted and created again since
     * the preview is refused.
     */
    INCARNATION,
    /**
     * The system exposes no such identity: the etag holds the configuration and the member set. A change of either is
     * refused, but a resource created again with the same configuration is not told apart from the one previewed.
     */
    CONFIGURATION
}
