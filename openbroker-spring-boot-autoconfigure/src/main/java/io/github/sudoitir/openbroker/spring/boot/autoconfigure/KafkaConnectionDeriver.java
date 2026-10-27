package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Secret;
import io.github.sudoitir.openbroker.TlsProvider;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

/**
 * The {@code kafka} connection from Spring Boot's {@link KafkaConnectionDetails}, which also come from
 * {@code @ServiceConnection} and Docker Compose. Only what the details state is derived: the bootstrap
 * servers, the security protocol and the SSL bundle, and, from {@code spring.kafka.properties}, the SASL
 * mechanism and the user name and password of a {@code PLAIN} or {@code SCRAM-*} login.
 */
final class KafkaConnectionDeriver implements ConnectionDeriver {

    static final String NAME = "kafka";

    private static final String KAFKA = "kafka";
    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_MECHANISM = "sasl.mechanism";
    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

    private final BrokerDrivers drivers;
    private final ObjectProvider<KafkaConnectionDetails> details;
    private final Environment environment;
    private final ObjectProvider<SslBundleTls> ssl;

    KafkaConnectionDeriver(
            BrokerDrivers drivers,
            ObjectProvider<KafkaConnectionDetails> details,
            Environment environment,
            ObjectProvider<SslBundleTls> ssl) {
        this.drivers = drivers;
        this.details = details;
        this.environment = environment;
        this.ssl = ssl;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public @Nullable Derived derive() {
        KafkaConnectionDetails available = details.getIfUnique();
        if (available == null) {
            return null;
        }
        KafkaConnectionDetails.Configuration admin = available.getAdmin();
        List<Endpoint> endpoints = new ArrayList<>();
        for (String server : admin.getBootstrapServers()) {
            if (StringUtils.hasText(server)) {
                endpoints.add(new Endpoint(Plane.DATA, KAFKA, server.trim()));
            }
        }
        Map<String, String> options = new LinkedHashMap<>();
        if (StringUtils.hasText(admin.getSecurityProtocol())) {
            options.put(SECURITY_PROTOCOL, admin.getSecurityProtocol());
        }
        Map<String, String> properties = properties();
        String mechanism = properties.get(SASL_MECHANISM);
        CredentialsProperties credentials = CredentialsProperties.unset();
        String needed = null;
        if (StringUtils.hasText(mechanism)) {
            String normalised = mechanism.trim().toUpperCase(Locale.ROOT);
            if (supported().contains(normalised)) {
                options.put(SASL_MECHANISM, normalised);
                credentials = login(normalised, properties.get(SASL_JAAS_CONFIG));
                if (!credentials.isSet()) {
                    needed = "Spring's Kafka properties use the SASL mechanism " + normalised
                            + ", whose credentials cannot be derived";
                }
            } else {
                needed = "Spring's Kafka properties use the SASL mechanism '" + mechanism.trim()
                        + "', which the Kafka driver does not support, so neither it nor its credentials can be derived";
            }
        }
        Connection connection = new Connection(
                KAFKA,
                new ClusterSpec(List.of(), endpoints, options),
                credentials,
                CredentialsProperties.unset(),
                ConnectionOptions.unset(),
                Connection.defaults().sender(),
                Connection.defaults().receiver(),
                Connection.defaults().processor());
        return new Derived(connection, tls(available, admin), needed);
    }

    /** The SASL mechanisms the driver accepts, as its schema declares them. */
    private List<String> supported() {
        return drivers.descriptor(KAFKA)
                .schema()
                .option(SASL_MECHANISM)
                .map(spec -> spec.allowedValues())
                .orElse(List.of());
    }

    /** {@code spring.kafka.properties}, with the admin client's own laid over them. */
    private Map<String, String> properties() {
        Binder binder = Binder.get(environment);
        Bindable<Map<String, String>> map = Bindable.mapOf(String.class, String.class);
        Map<String, String> merged =
                new LinkedHashMap<>(binder.bind("spring.kafka.properties", map).orElse(Map.of()));
        merged.putAll(binder.bind("spring.kafka.admin.properties", map).orElse(Map.of()));
        return merged;
    }

    /**
     * The user name and password of a {@code PLAIN} or {@code SCRAM-*} login, read from the JAAS
     * configuration with Kafka's own parser. Nothing of the configuration reaches a message.
     */
    private static CredentialsProperties login(String mechanism, @Nullable String jaas) {
        String upper = mechanism.toUpperCase(Locale.ROOT);
        if (jaas == null || !(upper.equals("PLAIN") || upper.startsWith("SCRAM-"))) {
            return CredentialsProperties.unset();
        }
        try {
            JaasContext context = JaasContext.loadClientContext(Map.of(SASL_JAAS_CONFIG, new Password(jaas)));
            for (AppConfigurationEntry entry : context.configurationEntries()) {
                Object user = entry.getOptions().get("username");
                Object password = entry.getOptions().get("password");
                if (user != null && password != null) {
                    return new CredentialsProperties(
                            CredentialsProperties.Form.USERNAME_PASSWORD,
                            user.toString(),
                            Secret.of(password.toString()),
                            null,
                            null);
                }
            }
        } catch (RuntimeException e) {
            // A configuration Kafka cannot parse is not derived; its text is not repeated here.
        }
        return CredentialsProperties.unset();
    }

    /** The bundle by name when the properties name it, so its reloads are heard; else the details' own. */
    private @Nullable TlsProvider tls(KafkaConnectionDetails available, KafkaConnectionDetails.Configuration admin) {
        SslBundleTls bundles = ssl.getIfAvailable();
        String name = environment.getProperty("spring.kafka.admin.ssl.bundle");
        if (!StringUtils.hasText(name)) {
            name = environment.getProperty("spring.kafka.ssl.bundle");
        }
        if (bundles != null && StringUtils.hasText(name)) {
            return bundles.forBundle(name, true);
        }
        if (admin.getSslBundle() == null || bundles == null) {
            return null;
        }
        return bundles.following(() -> available.getAdmin().getSslBundle(), true);
    }
}
