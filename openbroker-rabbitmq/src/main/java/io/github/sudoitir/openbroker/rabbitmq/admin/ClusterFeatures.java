package io.github.sudoitir.openbroker.rabbitmq.admin;

/**
 * What the cluster is and offers, read once per connection from {@code /api/overview}, {@code /api/nodes} and
 * {@code /api/whoami}: the version gate, the product, the plugins enabled on every node, the user's tags and the
 * feature assessments.
 */
final class ClusterFeatures {

    ClusterFeatures(RabbitMqCluster cluster) {}

    /** Fails {@code FAILED_PRECONDITION} when the version is outside {@code supportedVersions}; returns this otherwise. */
    ClusterFeatures require(String supportedVersions) {
        throw RabbitMqErrors.unimplemented("ClusterFeatures.require");
    }
}
