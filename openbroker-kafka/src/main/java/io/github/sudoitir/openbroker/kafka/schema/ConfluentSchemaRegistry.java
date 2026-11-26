package io.github.sudoitir.openbroker.kafka.schema;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallScope;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.schema.RegistryHttp.Redactor;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.operation.AttributeChange;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.EtagBasis;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * A Confluent-compatible schema registry. A preview writes nothing. An apply checks the preview's etag first and
 * writes only when it still matches.
 */
final class ConfluentSchemaRegistry implements SchemaCatalog {

    static final int PAGE = 100;
    private static final String CHECK_ON_APPLY = "The compatibility check happens on apply.";
    private static final String ALREADY = "Already registered; applying creates no second version.";
    private static final String CANNOT_RESTORE = "The schemas cannot be restored.";
    private static final String NOTHING_WRITTEN = "Nothing has been written.";
    private static final String SUBJECT = "subject";
    private static final String CONDITION = "condition";
    private static final String LEVEL = "level";
    private static final String COMPATIBILITY = "compatibility";
    private static final String SCHEMA = "schema";
    private static final String SCHEMA_TYPE = "schemaType";
    private static final String SUBJECTS = "/subjects/";
    private static final String VERSIONS = "/versions";

    private static final ObjectMapper JSON = JsonMapper.builder().build();

    private final KafkaCluster cluster;
    private final RegistryHttp http = new RegistryHttp();
    /** Last body read for a version. A soft-deleted version stays addressable when the registry will not return it again. */
    private final ConcurrentHashMap<String, SubjectVersion> knownVersions = new ConcurrentHashMap<>();

    ConfluentSchemaRegistry(KafkaCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Assessment capability() {
        if (rawUrl().isEmpty()) {
            return Assessment.unavailable("registry.url is not set on this connection.", Optional.empty());
        }
        try {
            RegistryHttp.Reply reply = get("/subjects");
            if (reply.status() >= 200 && reply.status() < 300) {
                return Assessment.available("The schema registry answered.");
            }
            if (reply.status() == 401 || reply.status() == 403) {
                return Assessment.unavailable("The registry refused the credentials.", Optional.empty());
            }
            return Assessment.unknown("The registry answered " + reply.status() + ".");
        } catch (BrokerException e) {
            return Assessment.unknown(e.getMessage());
        }
    }

    @Override
    public Subjects subjects(Cursor cursor) {
        Objects.requireNonNull(cursor, "cursor");
        List<String> names = names();
        int start = start(names, cursor.token());
        int end = Math.min(names.size(), start + PAGE);
        List<Subject> page = new ArrayList<>();
        for (int i = start; i < end; i++) {
            state(names.get(i)).ifPresent(found -> page.add(found.subject()));
        }
        Optional<Cursor> next = end < names.size() ? Optional.of(new Cursor(names.get(end - 1))) : Optional.empty();
        return new Subjects(page, next);
    }

    @Override
    public Subject subject(String name) {
        return state(required(name)).map(State::subject).orElseThrow(() -> gone(name));
    }

    @Override
    public Preview previewRegister(String subject, SchemaType type, String schema) {
        String name = required(subject);
        SchemaType kind = Objects.requireNonNull(type, "type");
        String text = requiredSchema(schema);
        State now = state(name).orElseGet(() -> State.absent(name));
        Optional<Integer> existing = lookup(name, kind, text);
        String check = compatibilityCheck(name, kind, text);
        List<AttributeChange> diff = new ArrayList<>();
        diff.add(new AttributeChange(SUBJECT, now.present() ? Optional.of(name) : Optional.empty(), Optional.of(name)));
        diff.add(new AttributeChange("schema.type", Optional.empty(), Optional.of(kind.name())));
        existing.ifPresent(id -> diff.add(new AttributeChange("schema.id", Optional.of(id), Optional.of(id))));
        diff.add(new AttributeChange(
                "note",
                Optional.empty(),
                Optional.of(NOTHING_WRITTEN + " " + (existing.isPresent() ? ALREADY : check))));
        return preview(diff, Set.of(), existing.isPresent() ? 0 : 1, now.etag());
    }

    @Override
    public SubjectVersion register(String subject, SchemaType type, String schema, Precondition condition) {
        String name = required(subject);
        SchemaType kind = Objects.requireNonNull(type, "type");
        String text = requiredSchema(schema);
        Objects.requireNonNull(condition, CONDITION);
        State now = state(name).orElseGet(() -> State.absent(name));
        requireMatch(condition, now.etag());
        RegistryHttp.Reply reply = post(SUBJECTS + enc(name) + VERSIONS, body(kind, text), true);
        if (reply.status() == 409) {
            throw refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.INVALID_ARGUMENT,
                    Dispatch.REJECTED,
                    registryMessage(reply));
        }
        require2xx(reply, "register " + name);
        int id = json(reply).path("id").asInt();
        return versionFor(name, id);
    }

    @Override
    public Preview previewCompatibility(String subject, CompatibilityLevel level) {
        String name = required(subject);
        Objects.requireNonNull(level, LEVEL);
        State now = state(name).orElseGet(() -> State.absent(name));
        List<AttributeChange> diff = List.of(
                new AttributeChange(COMPATIBILITY, now.level().map(Enum::name), Optional.of(level.name())),
                new AttributeChange("note", Optional.empty(), Optional.of(NOTHING_WRITTEN)));
        int affected = now.level().isPresent() && now.level().get() == level ? 0 : 1;
        return preview(diff, Set.of(), affected, now.etag());
    }

    @Override
    public CompatibilityLevel setCompatibility(String subject, CompatibilityLevel level, Precondition condition) {
        String name = required(subject);
        Objects.requireNonNull(level, LEVEL);
        Objects.requireNonNull(condition, CONDITION);
        State now = state(name).orElseGet(() -> State.absent(name));
        requireMatch(condition, now.etag());
        RegistryHttp.Reply reply = put("/config/" + enc(name), configBody(level), true);
        require2xx(reply, "set compatibility of " + name);
        return level;
    }

    @Override
    public Preview previewCompatibilityDefault(CompatibilityLevel level) {
        Objects.requireNonNull(level, LEVEL);
        String current = defaultLevel();
        List<AttributeChange> diff = List.of(
                new AttributeChange(
                        COMPATIBILITY,
                        current.isEmpty() ? Optional.empty() : Optional.of(current),
                        Optional.of(level.name())),
                new AttributeChange("note", Optional.empty(), Optional.of(NOTHING_WRITTEN)));
        return preview(diff, Set.of(), current.equals(level.name()) ? 0 : 1, SchemaEtags.of("", List.of(), current));
    }

    @Override
    public CompatibilityLevel setCompatibilityDefault(CompatibilityLevel level, Precondition condition) {
        Objects.requireNonNull(level, LEVEL);
        Objects.requireNonNull(condition, CONDITION);
        String current = defaultLevel();
        requireMatch(condition, SchemaEtags.of("", List.of(), current));
        RegistryHttp.Reply reply = put("/config", configBody(level), true);
        require2xx(reply, "set the registry default compatibility");
        return level;
    }

    @Override
    public Preview previewDelete(String subject, boolean permanent) {
        String name = required(subject);
        State now = state(name).orElseThrow(() -> gone(name));
        List<AttributeChange> diff = new ArrayList<>();
        diff.add(new AttributeChange(SUBJECT, Optional.of(name), Optional.empty()));
        String note = permanent ? NOTHING_WRITTEN + " " + CANNOT_RESTORE : NOTHING_WRITTEN;
        diff.add(new AttributeChange("note", Optional.empty(), Optional.of(note)));
        return preview(
                diff, permanent ? Set.of(SUBJECT) : Set.of(), now.versions().size(), now.etag());
    }

    @Override
    public void delete(String subject, boolean permanent, Precondition condition) {
        String name = required(subject);
        Objects.requireNonNull(condition, CONDITION);
        State now = state(name).orElseThrow(() -> gone(name));
        requireMatch(condition, now.etag());
        String path = SUBJECTS + enc(name);
        // A permanent delete is refused until the subject is soft-deleted. Soft-delete first, then remove it.
        RegistryHttp.Reply soft = exchange("DELETE", path, null, true);
        if (soft.status() < 200 || soft.status() >= 300) {
            // Refetch. The state read above is from before this DELETE, so it cannot say whether a 404 means the
            // subject is already off the live list.
            if (soft.status() == 404
                    && !state(name).orElseThrow(() -> gone(name)).live()) {
                // Already soft-deleted: the live list 404s and the deleted list still names it. Proceed.
            } else if (soft.status() == 404) {
                throw gone(name);
            } else {
                require2xx(soft, "delete " + name);
            }
        }
        if (!permanent) {
            return;
        }
        RegistryHttp.Reply hard = exchange("DELETE", path + "?permanent=true", null, true);
        if (hard.status() < 200 || hard.status() >= 300) {
            throw new BrokerException(
                    Status.ABORTED,
                    ErrorInfo.of(ErrorReasons.UNEXPECTED_SYSTEM),
                    Dispatch.IN_DOUBT,
                    null,
                    redactor()
                            .apply("The subject " + name
                                    + " was soft-deleted, and the permanent delete did not finish: "
                                    + registryMessage(hard)));
        }
    }

    @Override
    public DecodedRecord decode(String serdeName, MessageBody body) {
        if (serdeName != null && !SerdeRegistry.known(serdeName)) {
            return new DecodedRecord.Undecoded(DecodedRecord.SERDE_NOT_REGISTERED);
        }
        if (body.isTombstone()) {
            return new DecodedRecord.Undecoded(DecodedRecord.NO_SCHEMA_ID);
        }
        byte[] bytes = bytes(body);
        if (bytes.length == 0 || bytes[0] != 0) {
            return new DecodedRecord.Undecoded(DecodedRecord.NO_SCHEMA_ID);
        }
        if (bytes.length < 5) {
            return new DecodedRecord.Undecoded(DecodedRecord.TRUNCATED);
        }
        int id = ByteBuffer.wrap(bytes, 1, 4).getInt();
        Optional<Stored> stored = schema(id);
        if (stored.isEmpty()) {
            return new DecodedRecord.Undecoded(DecodedRecord.SCHEMA_NOT_FOUND);
        }
        byte[] payload = new byte[bytes.length - 5];
        System.arraycopy(bytes, 5, payload, 0, payload.length);
        return SerdeRegistry.decode(
                serdeName,
                stored.get().type(),
                id,
                stored.get().subject(),
                stored.get().schema(),
                payload);
    }

    @Override
    public byte[] encode(String serdeName, String subject, Map<String, ?> record) {
        if (serdeName == null || !SerdeRegistry.known(serdeName)) {
            throw refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    Dispatch.NOT_SENT,
                    "No serde named " + serdeName + " is registered.");
        }
        Objects.requireNonNull(record, "record");
        String schema;
        try {
            schema = SerdeRegistry.schemaOf(serdeName, record);
        } catch (IllegalArgumentException e) {
            throw refused(Status.INVALID_ARGUMENT, ErrorReasons.INVALID_ARGUMENT, Dispatch.NOT_SENT, e.getMessage());
        }
        SubjectVersion version = register(subject, SerdeRegistry.typeOf(serdeName), schema, Precondition.none());
        try {
            return SerdeRegistry.frame(serdeName, version.id(), schema, record);
        } catch (RuntimeException e) {
            throw refused(Status.INVALID_ARGUMENT, ErrorReasons.INVALID_ARGUMENT, Dispatch.NOT_SENT, e.getMessage());
        }
    }

    @Override
    public void close() {
        http.close();
    }

    private Optional<Stored> schema(int id) {
        RegistryHttp.Reply reply = get("/schemas/ids/" + id);
        if (reply.status() == 404) {
            return Optional.empty();
        }
        require2xx(reply, "read schema " + id);
        JsonNode node = json(reply);
        String subject = "";
        RegistryHttp.Reply versions = get("/schemas/ids/" + id + VERSIONS);
        if (versions.status() == 200) {
            JsonNode listed = json(versions);
            if (listed.isArray() && !listed.isEmpty()) {
                subject = listed.get(0).path(SUBJECT).asString();
            }
        }
        return Optional.of(new Stored(
                schemaType(node.path(SCHEMA_TYPE).asString()), node.path(SCHEMA).asString(), subject));
    }

    private List<String> names() {
        RegistryHttp.Reply reply = get("/subjects");
        require2xx(reply, "list subjects");
        JsonNode node = json(reply);
        if (!node.isArray()) {
            throw bad("The registry's subject list was not a list.");
        }
        List<String> names = new ArrayList<>();
        node.forEach(item -> names.add(item.asString()));
        names.sort(Comparator.naturalOrder());
        return names;
    }

    private Optional<State> state(String name) {
        RegistryHttp.Reply live = get(SUBJECTS + enc(name) + VERSIONS);
        RegistryHttp.Reply deleted = get(SUBJECTS + enc(name) + "/versions?deleted=true");
        boolean liveMissing = live.status() == 404;
        boolean deletedOk = deleted.status() >= 200 && deleted.status() < 300;
        if (liveMissing && !deletedOk) {
            return Optional.empty();
        }
        if (!liveMissing) {
            require2xx(live, "list versions of " + name);
        }
        List<Integer> current = liveMissing ? List.of() : ints(json(live));
        List<Integer> all = deletedOk ? ints(json(deleted)) : current;
        if (all.isEmpty() && liveMissing) {
            return Optional.empty();
        }
        List<SubjectVersion> versions = new ArrayList<>();
        for (int number : all) {
            boolean gone = !current.contains(number);
            versions.add(version(name, number, gone));
        }
        versions.sort(Comparator.comparingInt(SubjectVersion::version));
        Level level = level(name);
        return Optional.of(new State(name, versions, level.level(), level.note(), level.raw(), true, !liveMissing));
    }

    private SubjectVersion version(String name, int number, boolean deleted) {
        String path = SUBJECTS + enc(name) + "/versions/" + number + (deleted ? "?deleted=true" : "");
        RegistryHttp.Reply reply = get(path);
        if ((reply.status() < 200 || reply.status() >= 300) && deleted) {
            SubjectVersion cached = knownVersions.get(name + "\0" + number);
            if (cached != null) {
                return new SubjectVersion(cached.id(), cached.version(), cached.type(), cached.schema(), true);
            }
            throw refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.NOT_SUPPORTED,
                    Dispatch.REJECTED,
                    "The registry will not return the schema of " + name + " version " + number + ".");
        }
        require2xx(reply, "read " + name + " version " + number);
        JsonNode node = json(reply);
        SubjectVersion read = new SubjectVersion(
                node.path("id").asInt(),
                node.path("version").asInt(number),
                schemaType(node.path(SCHEMA_TYPE).asString()),
                node.path(SCHEMA).asString(),
                deleted || node.path("deleted").asBoolean(false));
        knownVersions.put(name + "\0" + read.version(), read);
        return read;
    }

    private Level level(String name) {
        RegistryHttp.Reply reply = get("/config/" + enc(name));
        if (reply.status() == 404) {
            return new Level(Optional.empty(), Optional.of("The subject follows the registry default."), "");
        }
        require2xx(reply, "read compatibility of " + name);
        return parseLevel(json(reply));
    }

    private String defaultLevel() {
        RegistryHttp.Reply reply = get("/config");
        if (reply.status() == 404) {
            return "";
        }
        require2xx(reply, "read the registry default compatibility");
        return parseLevel(json(reply)).raw();
    }

    private static Level parseLevel(JsonNode node) {
        String raw = node.path("compatibilityLevel").asString();
        if (raw.isEmpty()) {
            raw = node.path(COMPATIBILITY).asString();
        }
        if (raw.isEmpty()) {
            return new Level(Optional.empty(), Optional.of("The registry did not restate a compatibility level."), "");
        }
        try {
            return new Level(Optional.of(CompatibilityLevel.valueOf(raw)), Optional.empty(), raw);
        } catch (IllegalArgumentException _) {
            return new Level(Optional.of(CompatibilityLevel.NONE), Optional.of(raw), raw);
        }
    }

    /** The Confluent lookup. It returns an existing id and registers nothing. */
    private Optional<Integer> lookup(String name, SchemaType type, String schema) {
        RegistryHttp.Reply reply = post(SUBJECTS + enc(name), body(type, schema), false);
        if (reply.status() == 404 || reply.status() == 405) {
            return Optional.empty();
        }
        if (reply.status() >= 200 && reply.status() < 300) {
            return Optional.of(json(reply).path("id").asInt());
        }
        return Optional.empty();
    }

    private String compatibilityCheck(String name, SchemaType type, String schema) {
        RegistryHttp.Reply reply =
                post("/compatibility/subjects/" + enc(name) + "/versions/latest", body(type, schema), false);
        if (reply.status() == 405 || (reply.status() == 404 && !registryAnswer(reply))) {
            return CHECK_ON_APPLY;
        }
        if (reply.status() == 404) {
            return "The subject has no version yet; " + CHECK_ON_APPLY;
        }
        if (reply.status() == 409) {
            return "The registry would refuse this schema: " + registryMessage(reply);
        }
        if (reply.status() >= 200 && reply.status() < 300) {
            boolean compatible = json(reply).path("is_compatible").asBoolean(false);
            return compatible
                    ? "The registry judges this schema compatible."
                    : "The registry judges this schema incompatible.";
        }
        return CHECK_ON_APPLY;
    }

    private SubjectVersion versionFor(String name, int id) {
        State after = state(name).orElseThrow(() -> gone(name));
        return after.versions().stream()
                .filter(version -> version.id() == id)
                .findFirst()
                .orElseThrow(() -> bad("The registry registered schema id " + id + " but did not list it."));
    }

    private RegistryHttp.Reply get(String path) {
        return exchange("GET", path, null, false);
    }

    private RegistryHttp.Reply post(String path, byte[] body, boolean write) {
        return exchange("POST", path, body, write);
    }

    private RegistryHttp.Reply put(String path, byte[] body, boolean write) {
        return exchange("PUT", path, body, write);
    }

    private RegistryHttp.Reply exchange(String method, String path, byte[] body, boolean write) {
        Redactor redactor = redactor();
        Duration timeout = timeout();
        try {
            RegistryHttp.Reply reply = http.exchange(method, uri(path), body, redactor, write, timeout);
            if (reply.status() >= 300 && reply.status() < 400) {
                throw refused(
                        Status.FAILED_PRECONDITION,
                        ErrorReasons.ENDPOINT_NOT_FOUND,
                        Dispatch.REJECTED,
                        "The registry redirected the request. This client does not follow redirects.");
            }
            if (reply.status() == 401 || reply.status() == 403) {
                throw refused(
                        Status.UNAUTHENTICATED,
                        ErrorReasons.CREDENTIALS_REJECTED,
                        Dispatch.REJECTED,
                        "The registry refused the credentials.");
            }
            if (reply.status() >= 500) {
                RegistryHttp.logFailure(redactor, reply.status() + " " + registryMessage(reply));
            }
            return reply;
        } catch (BrokerException e) {
            RegistryHttp.logFailure(redactor, e.getMessage());
            throw e;
        }
    }

    private void require2xx(RegistryHttp.Reply reply, String what) {
        if (reply.status() == 405) {
            throw refused(
                    Status.UNIMPLEMENTED,
                    ErrorReasons.NOT_SUPPORTED,
                    Dispatch.REJECTED,
                    "The registry does not offer this operation: " + registryMessage(reply));
        }
        if (reply.status() < 200 || reply.status() >= 300) {
            throw refused(
                    Status.UNAVAILABLE,
                    ErrorReasons.UNEXPECTED_SYSTEM,
                    Dispatch.REJECTED,
                    "The registry could not " + what + ": " + registryMessage(reply));
        }
    }

    private boolean registryAnswer(RegistryHttp.Reply reply) {
        try {
            return json(reply).has("error_code");
        } catch (BrokerException _) {
            return false;
        }
    }

    private String registryMessage(RegistryHttp.Reply reply) {
        try {
            JsonNode node = json(reply);
            String message = node.path("message").asString();
            return message.isEmpty() ? "the registry answered " + reply.status() : message;
        } catch (BrokerException _) {
            return "the registry answered " + reply.status();
        }
    }

    private JsonNode json(RegistryHttp.Reply reply) {
        try {
            return JSON.readTree(reply.body());
        } catch (JacksonException _) {
            throw bad("The registry's answer was not JSON.");
        }
    }

    private List<Integer> ints(JsonNode node) {
        if (!node.isArray()) {
            throw bad("The registry's version list was not a list.");
        }
        List<Integer> out = new ArrayList<>();
        node.forEach(item -> out.add(item.asInt()));
        return out;
    }

    private byte[] body(SchemaType type, String schema) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(SCHEMA, schema);
        fields.put(SCHEMA_TYPE, type.name());
        return JSON.writeValueAsBytes(fields);
    }

    private byte[] configBody(CompatibilityLevel level) {
        return JSON.writeValueAsBytes(Map.of(COMPATIBILITY, level.name()));
    }

    private URI uri(String path) {
        String base = url();
        try {
            return URI.create(base + path);
        } catch (IllegalArgumentException _) {
            throw refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    Dispatch.NOT_SENT,
                    "registry.url is not a URL.");
        }
    }

    private String url() {
        String raw = rawUrl().orElseThrow(() -> refused(
                Status.INVALID_ARGUMENT,
                ErrorReasons.INVALID_ARGUMENT,
                Dispatch.NOT_SENT,
                "registry.url is not set on this connection."));
        URI uri;
        try {
            uri = URI.create(raw);
        } catch (IllegalArgumentException _) {
            throw refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    Dispatch.NOT_SENT,
                    "registry.url is not a URL.");
        }
        if (uri.getUserInfo() != null) {
            throw refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    Dispatch.NOT_SENT,
                    "registry.url must not carry credentials; use registry.credentials.");
        }
        String scheme = uri.getScheme();
        if (scheme == null || (!scheme.equals("http") && !scheme.equals("https"))) {
            throw refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    Dispatch.NOT_SENT,
                    "registry.url must be http or https.");
        }
        return raw.endsWith("/") ? raw.substring(0, raw.length() - 1) : raw;
    }

    private Optional<String> rawUrl() {
        String raw = cluster.option(KafkaOptions.REGISTRY_URL, "").trim();
        return raw.isEmpty() ? Optional.empty() : Optional.of(raw);
    }

    private Redactor redactor() {
        String raw = cluster.option(KafkaOptions.REGISTRY_CREDENTIALS, "");
        if (raw.isEmpty()) {
            return Redactor.none();
        }
        int colon = raw.indexOf(':');
        if (colon < 0) {
            throw refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    Dispatch.NOT_SENT,
                    "registry.credentials must be user:password.");
        }
        return Redactor.of(raw.substring(0, colon), raw.substring(colon + 1));
    }

    private Duration timeout() {
        Duration timeout = RegistryHttp.REQUEST;
        Optional<Deadline> deadline = CallScope.deadline();
        if (deadline.isPresent()) {
            Duration left = deadline.get().remaining(cluster.options().clock());
            if (left.compareTo(timeout) < 0) {
                timeout = left;
            }
        }
        if (timeout.isZero() || timeout.isNegative()) {
            throw refused(
                    Status.DEADLINE_EXCEEDED, "DEADLINE_EXCEEDED", Dispatch.NOT_SENT, "The call's deadline passed");
        }
        return timeout;
    }

    private void requireMatch(Precondition condition, String etag) {
        switch (condition) {
            case Precondition.None() -> {
                // No etag was taken, so the change runs.
            }
            case Precondition.IfMatch(var given)
            when !given.equals(etag) ->
                throw refused(
                        Status.FAILED_PRECONDITION,
                        ErrorReasons.PRECONDITION_FAILED,
                        Dispatch.NOT_SENT,
                        "The registry changed since the preview.");
            case Precondition.IfMatch(var _) -> {
                // The preview's etag still matches.
            }
        }
    }

    private BrokerException refused(Status status, String reason, Dispatch dispatch, String message) {
        return RegistryHttp.failed(status, reason, dispatch, redactor(), message);
    }

    private BrokerException gone(String name) {
        return refused(
                Status.NOT_FOUND,
                ErrorReasons.RESOURCE_GONE,
                Dispatch.NOT_SENT,
                "The subject " + name + " does not exist.");
    }

    private BrokerException bad(String message) {
        return refused(Status.INTERNAL, ErrorReasons.BAD_RESPONSE, Dispatch.REJECTED, message);
    }

    private String required(String name) {
        if (name == null || name.isBlank()) {
            throw refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    Dispatch.NOT_SENT,
                    "A subject name is required.");
        }
        return name;
    }

    private String requiredSchema(String schema) {
        if (schema == null || schema.isBlank()) {
            throw refused(
                    Status.INVALID_ARGUMENT, ErrorReasons.INVALID_ARGUMENT, Dispatch.NOT_SENT, "A schema is required.");
        }
        return schema;
    }

    private static String enc(String name) {
        return URLEncoder.encode(name, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static SchemaType schemaType(String raw) {
        if (raw == null || raw.isBlank()) {
            return SchemaType.AVRO;
        }
        try {
            return SchemaType.valueOf(raw);
        } catch (IllegalArgumentException _) {
            return SchemaType.AVRO;
        }
    }

    private static int start(List<String> names, String token) {
        if (token == null || token.isEmpty()) {
            return 0;
        }
        int at = names.indexOf(token);
        if (at >= 0) {
            return at + 1;
        }
        int start = 0;
        while (start < names.size() && names.get(start).compareTo(token) <= 0) {
            start++;
        }
        return start;
    }

    private byte[] bytes(MessageBody body) {
        if (body.bytes().isPresent()) {
            return body.bytes().get().toByteArray();
        }
        try (InputStream in = body.openStream()) {
            return in.readAllBytes();
        } catch (IOException _) {
            throw refused(
                    Status.UNAVAILABLE, ErrorReasons.UNREACHABLE, Dispatch.NOT_SENT, "The record could not be read.");
        }
    }

    private static Preview preview(List<AttributeChange> diff, Set<String> replace, int versions, String etag) {
        return new Preview(
                diff,
                replace,
                new Estimate(Count.exact(versions), "version"),
                etag,
                EtagBasis.CONFIGURATION,
                PreconditionEnforcement.CHECKED_BEFORE);
    }

    private record State(
            String name,
            List<SubjectVersion> versions,
            Optional<CompatibilityLevel> level,
            Optional<String> note,
            String raw,
            boolean present,
            boolean live) {

        static State absent(String name) {
            return new State(name, List.of(), Optional.empty(), Optional.empty(), "", false, false);
        }

        Subject subject() {
            return new Subject(name, versions, level, note);
        }

        String etag() {
            return SchemaEtags.of(name, versions, raw);
        }
    }

    private record Level(Optional<CompatibilityLevel> level, Optional<String> note, String raw) {}

    private record Stored(SchemaType type, String schema, String subject) {}
}
