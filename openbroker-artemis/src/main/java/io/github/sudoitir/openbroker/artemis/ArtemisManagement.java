package io.github.sudoitir.openbroker.artemis;

/**
 * What only Artemis can do, reached through {@code ClusterConnection#unwrap(ArtemisManagement.class)}:
 * broker.xml-level settings and configuration reads that have no neutral meaning. Code that uses it is
 * Artemis-specific by definition; keep it in one place.
 */
public interface ArtemisManagement {}
