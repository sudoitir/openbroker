package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.EtagBasis;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

/**
 * Broker configuration: one resource per broker, and {@code *} for the cluster-wide default, which Kafka
 * keeps under a broker resource with an empty name. A listing reads all of a page with one {@code
 * describeConfigs}. An update changes dynamic settings with {@code incrementalAlterConfigs}; a setting that
 * only a restart changes, or that holds a secret, is refused before anything is sent.
 */
final class BrokerConfigs implements Handler {

    /** The id name of the cluster-wide default. */
    static final String DEFAULT = "*";

    private final Kind kind;

    BrokerConfigs(Kind kind) {
        this.kind = kind;
    }

    @Override
    public Kind kind() {
        return kind;
    }

    @Override
    public Set<String> selectors() {
        return Set.of();
    }

    static ResourceId id(String name) {
        return new ResourceId(KafkaKinds.BROKER_CONFIG, ResourcePath.of(name));
    }

    // Reading.

    @Override
    public List<Candidate> candidates(Context context, Listing listing) {
        var match = listing.names();
        List<Candidate> out = new ArrayList<>();
        if (match.test(DEFAULT)) {
            out.add(new Candidate(
                    id(DEFAULT),
                    List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                    Attributes.empty()));
        }
        for (Node node : brokers(context)) {
            if (match.test(node.idString())) {
                out.add(new Candidate(
                        id(node.idString()),
                        List.of(Placement.instance(NodeId.of(node.idString()), Attributes.empty())),
                        Attributes.empty()));
            }
        }
        out.sort(Comparator.comparing(BrokerConfigs::order));
        return out;
    }

    /** The default first, then the brokers by number. */
    private static Long order(Candidate candidate) {
        String name = candidate.id().name();
        return name.equals(DEFAULT) ? Long.MIN_VALUE : Long.parseLong(name);
    }

    @Override
    public List<Resource> resources(Context context, List<Candidate> page, Listing listing) {
        if (page.isEmpty()) {
            return List.of();
        }
        Map<String, Config> described =
                describe(context, page.stream().map(c -> c.id().name()).toList());
        List<Resource> out = new ArrayList<>();
        for (Candidate candidate : page) {
            Config config = described.get(candidate.id().name());
            if (config != null) {
                out.add(resource(context, candidate, config, listing));
            }
        }
        return out;
    }

    @Override
    public Optional<Resource> find(Context context, ResourceId id) {
        String name = name(id);
        Candidate candidate = candidateOf(context, name);
        if (candidate == null) {
            return Optional.empty();
        }
        Config config = describe(context, List.of(name)).get(name);
        return Optional.ofNullable(config)
                .map(c -> resource(context, candidate, c, new Listing(ResourceQuery.of(KafkaKinds.BROKER_CONFIG))));
    }

    private static Candidate candidateOf(Context context, String name) {
        if (name.equals(DEFAULT)) {
            return new Candidate(
                    id(DEFAULT),
                    List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                    Attributes.empty());
        }
        return brokers(context).stream()
                .filter(n -> n.idString().equals(name))
                .findFirst()
                .map(n -> new Candidate(
                        id(name), List.of(Placement.instance(NodeId.of(name), Attributes.empty())), Attributes.empty()))
                .orElse(null);
    }

    private static List<Node> brokers(Context context) {
        return List.copyOf(context.await()
                .get(context.admin()
                        .describeCluster(new DescribeClusterOptions()
                                .timeoutMs(context.await().timeoutMs()))
                        .nodes()));
    }

    /** The id's name: {@code *}, or a broker number. */
    private static String name(ResourceId id) {
        String name = id.name();
        if (!name.equals(DEFAULT)) {
            try {
                Long.parseLong(name);
            } catch (NumberFormatException _) {
                throw Attrs.invalid("A broker configuration is named by a broker number or *: " + id);
            }
        }
        return name;
    }

    /** One {@code describeConfigs} for every name, keyed by the id's name. */
    private static Map<String, Config> describe(Context context, List<String> names) {
        Map<ConfigResource, String> asked = new LinkedHashMap<>();
        for (String name : names) {
            asked.put(new ConfigResource(ConfigResource.Type.BROKER, name.equals(DEFAULT) ? "" : name), name);
        }
        Map<ConfigResource, Config> all = context.await()
                .get(context.admin()
                        .describeConfigs(
                                asked.keySet(),
                                new DescribeConfigsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
        Map<String, Config> out = new HashMap<>();
        all.forEach((resource, config) -> out.put(asked.getOrDefault(resource, resource.name()), config));
        return out;
    }

    private static Resource resource(Context context, Candidate candidate, Config config, Listing listing) {
        Attributes.Builder attributes = Attributes.builder();
        Set<String> readOnly = new TreeSet<>();
        Map<String, String> sources = new TreeMap<>();
        for (ConfigEntry entry : config.entries()) {
            if (entry.isSensitive()) {
                continue;
            }
            if (entry.value() != null) {
                attributes.putRaw(KafkaKinds.CONFIG_PREFIX + entry.name(), entry.value());
            }
            if (entry.isReadOnly()) {
                readOnly.add(entry.name());
            }
            String source = source(entry.source());
            if (!source.isEmpty()) {
                sources.put(entry.name(), source);
            }
        }
        if (!candidate.id().name().equals(DEFAULT)) {
            attributes.putRaw(KafkaKinds.CONFIG_READ_ONLY, String.join(",", readOnly));
        }
        attributes.putRaw(
                KafkaKinds.CONFIG_SOURCES,
                sources.entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(",")));
        return new Resource(
                candidate.id(),
                Attrs.project(attributes.build(), listing.query().projection()),
                List.of(),
                candidate.placements(),
                context.now());
    }

    /** Where an overridden value comes from, compactly; empty for a built-in default. */
    static String source(ConfigEntry.ConfigSource source) {
        return switch (source) {
            case DYNAMIC_BROKER_CONFIG -> "node";
            case DYNAMIC_DEFAULT_BROKER_CONFIG -> "cluster";
            case STATIC_BROKER_CONFIG -> "static";
            case DEFAULT_CONFIG -> "";
            default -> source.name().toLowerCase(Locale.ROOT);
        };
    }

    // Updating.

    /** What an update would send, and what the target holds now. */
    private record Plan(Config current, List<AlterConfigOp> ops) {}

    @Override
    public Preview preview(Context context, ResourceId id, Operation operation, Scope scope) {
        if (!(operation instanceof Operation.Update update)) {
            throw unsupported(operation);
        }
        Plan plan = plan(context, id, update.patch());
        Attributes.Builder before = Attributes.builder();
        for (String key : update.patch().asMap().keySet()) {
            if (key.startsWith(KafkaKinds.CONFIG_PREFIX)) {
                ConfigEntry entry = plan.current().get(key.substring(KafkaKinds.CONFIG_PREFIX.length()));
                if (entry != null && entry.value() != null) {
                    before.putRaw(key, entry.value());
                }
            }
        }
        return new Preview(
                Attrs.diff(before.build(), update.patch()),
                Set.of(),
                new Estimate(Count.exact(1), "{configuration}"),
                etag(context, id, operation, scope),
                EtagBasis.CONFIGURATION,
                PreconditionEnforcement.CHECKED_BEFORE);
    }

    @Override
    public OperationResult execute(Context context, ResourceId id, Operation operation, Scope scope) {
        if (!(operation instanceof Operation.Update update)) {
            throw unsupported(operation);
        }
        Plan plan = plan(context, id, update.patch());
        if (plan.ops().isEmpty()) {
            return OperationResult.already();
        }
        String name = name(id);
        context.await()
                .get(context.admin()
                        .incrementalAlterConfigs(
                                Map.of(resourceOf(name), plan.ops()),
                                new AlterConfigsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
        Settle.until(context, () -> shows(context, name, plan.ops()));
        return OperationResult.done();
    }

    private static ConfigResource resourceOf(String name) {
        return new ConfigResource(ConfigResource.Type.BROKER, name.equals(DEFAULT) ? "" : name);
    }

    /** Whether the target shows every change the update made. */
    private static boolean shows(Context context, String name, List<AlterConfigOp> ops) {
        Config config = describe(context, List.of(name)).get(name);
        ConfigEntry.ConfigSource dynamic = name.equals(DEFAULT)
                ? ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG
                : ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
        for (AlterConfigOp op : ops) {
            ConfigEntry entry = config.get(op.configEntry().name());
            boolean agrees = op.opType() == AlterConfigOp.OpType.SET
                    ? entry != null && op.configEntry().value().equals(entry.value())
                    : entry == null || entry.source() != dynamic;
            if (!agrees) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads the target once and refuses a patch that names a setting a restart changes, or one that holds a
     * secret, before anything is written.
     */
    private Plan plan(Context context, ResourceId id, Attributes patch) {
        String name = name(id);
        Map<String, String> changes = new LinkedHashMap<>();
        for (Map.Entry<String, Object> change : patch.asMap().entrySet()) {
            String key = change.getKey();
            if (key.startsWith(KafkaKinds.CONFIG_PREFIX)) {
                String setting = key.substring(KafkaKinds.CONFIG_PREFIX.length());
                if (looksSensitive(setting)) {
                    throw sensitive(setting);
                }
                changes.put(setting, String.valueOf(change.getValue()));
            } else if (!key.startsWith("openbroker.")) {
                throw Attrs.invalid("A broker configuration has no attribute " + key + " to change.");
            }
        }
        List<Node> nodes = brokers(context);
        if (!name.equals(DEFAULT) && nodes.stream().noneMatch(n -> n.idString().equals(name))) {
            throw Handler.gone(id);
        }
        // The cluster-wide default lists only what is set, so which settings need a restart comes from a broker.
        List<String> asked = name.equals(DEFAULT) && !nodes.isEmpty()
                ? List.of(DEFAULT, nodes.getFirst().idString())
                : List.of(name);
        Map<String, Config> described = describe(context, asked);
        Config current = described.get(name);
        Config reference = described.get(asked.getLast());
        for (String setting : changes.keySet()) {
            ConfigEntry known = reference.get(setting);
            if (known != null && known.isSensitive()) {
                throw sensitive(setting);
            }
            if (known != null && known.isReadOnly()) {
                throw KafkaErrors.refused(
                        Status.FAILED_PRECONDITION,
                        ErrorReasons.IMMUTABLE_FIELD,
                        setting + " is read-only while a broker runs. Restart the broker with the new value in"
                                + " server.properties.");
            }
        }
        List<AlterConfigOp> ops = new ArrayList<>();
        changes.forEach((setting, value) -> ops.add(
                value.isEmpty()
                        ? new AlterConfigOp(new ConfigEntry(setting, null), AlterConfigOp.OpType.DELETE)
                        : new AlterConfigOp(new ConfigEntry(setting, value), AlterConfigOp.OpType.SET)));
        return new Plan(current, ops);
    }

    /** A setting whose name says it holds a secret, for a broker that does not list it. */
    private static boolean looksSensitive(String setting) {
        String lower = setting.toLowerCase(Locale.ROOT);
        return lower.contains("password")
                || lower.contains("secret")
                || lower.contains("jaas.config")
                || lower.endsWith(".key");
    }

    private static BrokerException sensitive(String setting) {
        return KafkaErrors.refused(
                Status.INVALID_ARGUMENT,
                ErrorReasons.INVALID_ARGUMENT,
                setting + " holds a secret and is not read or changed through resources.");
    }
}
