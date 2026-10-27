package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Secret;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.artemis.autoconfigure.ArtemisConnectionDetails;
import org.springframework.boot.artemis.autoconfigure.ArtemisMode;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * The {@code artemis} connection from Spring Boot's {@link ArtemisConnectionDetails}, which also come from
 * {@code @ServiceConnection} and Docker Compose: the Core endpoint on the data plane and the user and
 * password as data-plane credentials. Spring knows no management endpoint, so the management facets stay
 * unavailable until the connection adds a Jolokia endpoint and management credentials. An embedded
 * broker has no URL and is not derived.
 */
final class ArtemisConnectionDeriver implements ConnectionDeriver {

    static final String NAME = "artemis";

    private static final String NODE = "artemis";
    private static final String CORE = "core";

    private final ObjectProvider<ArtemisConnectionDetails> details;
    private final Environment environment;

    ArtemisConnectionDeriver(ObjectProvider<ArtemisConnectionDetails> details, Environment environment) {
        this.details = details;
        this.environment = environment;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public @Nullable Derived derive() {
        ArtemisConnectionDetails available = details.getIfUnique();
        if (available == null
                || embedded(available.getMode(), environment)
                || !StringUtils.hasText(available.getBrokerUrl())) {
            return null;
        }
        NodeSpec node =
                new NodeSpec(NodeId.of(NODE), List.of(new Endpoint(Plane.DATA, CORE, available.getBrokerUrl())));
        CredentialsProperties data = available.getUser() == null
                ? CredentialsProperties.unset()
                : new CredentialsProperties(
                        CredentialsProperties.Form.USERNAME_PASSWORD,
                        available.getUser(),
                        Secret.of(available.getPassword() == null ? "" : available.getPassword()),
                        null,
                        null);
        Connection connection = new Connection(
                NAME,
                new ClusterSpec(List.of(node), List.of(), Map.of()),
                data,
                new CredentialsProperties(CredentialsProperties.Form.NONE, null, null, null, null),
                ConnectionOptions.unset(),
                Connection.defaults().sender(),
                Connection.defaults().receiver(),
                Connection.defaults().processor());
        return new Derived(connection, null, null);
    }

    /**
     * Whether Spring runs the broker inside the application: the details say so, or say nothing and an
     * embedded broker is on the class path and enabled, as Spring itself decides.
     */
    static boolean embedded(@Nullable ArtemisMode mode, Environment environment) {
        if (mode != null) {
            return mode == ArtemisMode.EMBEDDED;
        }
        return embeddedByDefault(environment);
    }

    /** Spring's deduction when no mode is stated: embedded when an embedded server is on the class path. */
    static boolean embeddedByDefault(Environment environment) {
        return environment.getProperty("spring.artemis.embedded.enabled", Boolean.class, true)
                && embeddedServerPresent();
    }

    private static boolean embeddedServerPresent() {
        return ClassUtils.isPresent(
                        "org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS",
                        ArtemisConnectionDeriver.class.getClassLoader())
                || ClassUtils.isPresent(
                        "org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ",
                        ArtemisConnectionDeriver.class.getClassLoader());
    }
}
