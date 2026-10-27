package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

/**
 * ACLs: the access rules. A rule is identified by what it says, so its id is its seven parts and a
 * rule is made and removed, never changed.
 */
final class Acls implements Handler {

    private static final int PARTS = 7;

    private final Kind kind;

    Acls(Kind kind) {
        this.kind = kind;
    }

    @Override
    public Kind kind() {
        return kind;
    }

    @Override
    public Set<String> selectors() {
        return Set.of("principal", "resource_type", "resource_name", "operation", "permission");
    }

    static ResourceId id(AclBinding binding) {
        AccessControlEntry entry = binding.entry();
        ResourcePattern pattern = binding.pattern();
        return new ResourceId(
                KafkaKinds.ACL,
                ResourcePath.of(
                        entry.principal(),
                        entry.host(),
                        entry.operation().name().toLowerCase(Locale.ROOT),
                        entry.permissionType().name().toLowerCase(Locale.ROOT),
                        pattern.resourceType().name().toLowerCase(Locale.ROOT),
                        pattern.patternType().name().toLowerCase(Locale.ROOT),
                        pattern.name()));
    }

    /** The rule an id says. */
    static AclBinding binding(ResourceId id) {
        List<String> parts = id.path().segments();
        if (parts.size() != PARTS) {
            throw Attrs.invalid(
                    "An ACL is [principal, host, operation, permission, resource type, pattern type, resource name]: "
                            + id);
        }
        AclBinding binding = new AclBinding(
                new ResourcePattern(
                        ResourceType.fromString(parts.get(4).toUpperCase(Locale.ROOT)),
                        parts.get(6),
                        PatternType.fromString(parts.get(5).toUpperCase(Locale.ROOT))),
                new AccessControlEntry(
                        parts.get(0),
                        parts.get(1),
                        AclOperation.fromString(parts.get(2).toUpperCase(Locale.ROOT)),
                        AclPermissionType.fromString(parts.get(3).toUpperCase(Locale.ROOT))));
        if (binding.isUnknown()) {
            throw Attrs.invalid("Not an ACL: " + id + ".");
        }
        return binding;
    }

    private static Attributes attributes(AclBinding binding) {
        AccessControlEntry entry = binding.entry();
        return Attributes.builder()
                .putRaw(KafkaKinds.PRINCIPAL, entry.principal())
                .putRaw(KafkaKinds.HOST, entry.host())
                .putRaw(KafkaKinds.OPERATION, entry.operation().name().toLowerCase(Locale.ROOT))
                .putRaw(KafkaKinds.PERMISSION, entry.permissionType().name().toLowerCase(Locale.ROOT))
                .putRaw(
                        KafkaKinds.RESOURCE_TYPE,
                        binding.pattern().resourceType().name().toLowerCase(Locale.ROOT))
                .putRaw(
                        KafkaKinds.PATTERN_TYPE,
                        binding.pattern().patternType().name().toLowerCase(Locale.ROOT))
                .putRaw(KafkaKinds.RESOURCE_NAME, binding.pattern().name())
                .build();
    }

    private static List<AclBinding> all(Context context) {
        return List.copyOf(context.await()
                .get(context.admin()
                        .describeAcls(
                                AclBindingFilter.ANY,
                                new DescribeAclsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .values()));
    }

    @Override
    public List<Candidate> candidates(Context context, Listing listing) {
        var match = listing.names();
        List<Candidate> out = new ArrayList<>();
        for (AclBinding binding : all(context)) {
            Attributes attributes = attributes(binding);
            boolean selected = true;
            for (String key : selectors()) {
                Optional<String> wanted = listing.field(key);
                String have = String.valueOf(attributes.raw("kafka." + key).orElse(""));
                selected &= wanted.map(have::equalsIgnoreCase).orElse(true);
            }
            if (selected && match.test(binding.pattern().name())) {
                out.add(new Candidate(
                        id(binding),
                        List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                        attributes));
            }
        }
        out.sort(Comparator.comparing(c -> c.id().format()));
        return out;
    }

    @Override
    public List<Resource> resources(Context context, List<Candidate> page, Listing listing) {
        return page.stream()
                .map(c -> new Resource(
                        c.id(),
                        Attrs.project(c.attributes(), listing.query().projection()),
                        List.of(),
                        c.placements(),
                        context.now()))
                .toList();
    }

    @Override
    public Optional<Resource> find(Context context, ResourceId id) {
        AclBinding wanted = binding(id);
        return all(context).stream()
                .filter(wanted::equals)
                .findFirst()
                .map(b -> new Resource(
                        id,
                        attributes(b),
                        List.of(),
                        List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                        context.now()));
    }

    @Override
    public Preview preview(Context context, ResourceId id, Operation operation, Scope scope) {
        AclBinding binding = binding(id);
        boolean exists = find(context, id).isPresent();
        return switch (operation) {
            case Operation.Create create ->
                new Preview(
                        exists ? List.of() : Attrs.diff(Attributes.empty(), attributes(binding)),
                        Set.of(),
                        new Estimate(Count.exact(exists ? 0 : 1), "{acl}"),
                        Etags.of(id.format(), exists),
                        PreconditionEnforcement.CHECKED_BEFORE);
            case Operation.Delete delete ->
                new Preview(
                        exists ? Attrs.diff(attributes(binding), Attributes.empty()) : List.of(),
                        Set.of(),
                        new Estimate(Count.exact(exists ? 1 : 0), "{acl}"),
                        Etags.of(id.format(), exists),
                        PreconditionEnforcement.CHECKED_BEFORE);
            default -> throw unsupported(operation);
        };
    }

    @Override
    public String etag(Context context, ResourceId id, Operation operation, Scope scope) {
        return Etags.of(id.format(), find(context, id).isPresent());
    }

    @Override
    public OperationResult execute(Context context, ResourceId id, Operation operation, Scope scope) {
        AclBinding binding = binding(id);
        int timeout = context.await().timeoutMs();
        switch (operation) {
            case Operation.Create create -> {
                if (find(context, id).isPresent()) {
                    return OperationResult.already();
                }
                context.await()
                        .get(context.admin()
                                .createAcls(List.of(binding), new CreateAclsOptions().timeoutMs(timeout))
                                .all());
                Settle.until(context, () -> find(context, id).isPresent());
                return OperationResult.done();
            }
            case Operation.Delete delete -> {
                var removed = context.await()
                        .get(context.admin()
                                .deleteAcls(List.of(binding.toFilter()), new DeleteAclsOptions().timeoutMs(timeout))
                                .all());
                Settle.until(context, () -> find(context, id).isEmpty());
                return removed.isEmpty() ? OperationResult.already() : OperationResult.affected(removed.size());
            }
            default -> throw unsupported(operation);
        }
    }
}
