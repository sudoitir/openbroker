package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Direction;
import io.github.sudoitir.openbroker.operation.Election;
import io.github.sudoitir.openbroker.operation.EtagBasis;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import io.github.sudoitir.openbroker.operation.Resolution;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class RabbitMqOperationsTest {

    private static final Map<String, String> JSON = Map.of("Content-Type", "application/json");
    private static final String QUEUE = "/api/queues/%2F/orders";
    private static final ResourceId CLASSIC = ResourceId.of("rabbitmq.classic-queue", "/", "orders");
    private static final ResourceId QUORUM = ResourceId.of("rabbitmq.quorum-queue", "/", "orders");
    private static final ResourceId STREAM = ResourceId.of("rabbitmq.stream", "/", "orders");

    private FakeManagement server;
    private RabbitMqOperations operations;

    @BeforeEach
    void start() {
        server = new FakeManagement();
        operations = new RabbitMqOperations(FakeManagement.cluster(server.endpoint()));
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private void get(String path, String json) {
        server.respond("GET", path, 200, JSON, json);
    }

    private void ok(String method, String path) {
        server.respond(method, path, 204, Map.of(), "");
    }

    private static String queue(String type, int messages, int consumers, String members) {
        return "{\"name\":\"orders\",\"vhost\":\"/\",\"type\":\"" + type + "\",\"durable\":true,\"auto_delete\":false,"
                + "\"exclusive\":false,\"arguments\":{\"x-queue-type\":\"" + type + "\",\"x-max-length\":10},"
                + "\"messages\":" + messages + ",\"messages_ready\":" + messages + ",\"consumers\":" + consumers
                + ",\"members\":" + members + ",\"leader\":\"rabbit@a\"}";
    }

    private List<FakeManagement.Received> writes() {
        return server.received().stream().filter(r -> !"GET".equals(r.method())).toList();
    }

    private static JsonNode body(FakeManagement.Received request) {
        return JsonMapper.builder().build().readTree(request.body());
    }

    private Partial<OperationResult> run(ResourceId id, Operation operation) {
        return operations.execute(id, operation, Scope.whole());
    }

    private static BrokerException failure(Partial<OperationResult> result) {
        assertThat(result.failures()).hasSize(1);
        return result.failures().values().iterator().next();
    }

    @Test
    void aPreviewCarriesTheEtagTheEstimateAndTheBasisAndMakesNoWrite() {
        get(QUEUE, queue("quorum", 7, 0, "[\"rabbit@a\",\"rabbit@b\"]"));
        Preview preview = operations.preview(QUORUM, Operation.delete(), Scope.whole());
        assertThat(preview.etag()).isNotBlank();
        assertThat(preview.basis()).isEqualTo(EtagBasis.CONFIGURATION);
        assertThat(preview.enforcement()).isEqualTo(PreconditionEnforcement.CHECKED_BEFORE);
        assertThat(preview.estimate().count()).isEqualTo(Count.estimate(7));
        assertThat(preview.diff()).extracting("key").contains("openbroker.durable", "rabbitmq.argument.x-max-length");
        assertThat(writes()).isEmpty();
    }

    @Test
    void theEtagIgnoresVolumeAndLeaderButNotMembers() {
        get(QUEUE, queue("quorum", 1, 0, "[\"rabbit@a\",\"rabbit@b\"]"));
        get(QUEUE, queue("quorum", 9, 2, "[\"rabbit@b\",\"rabbit@a\"]"));
        get(QUEUE, queue("quorum", 9, 2, "[\"rabbit@a\",\"rabbit@c\"]"));
        String first =
                operations.preview(QUORUM, Operation.purge(), Scope.whole()).etag();
        String grown =
                operations.preview(QUORUM, Operation.purge(), Scope.whole()).etag();
        String moved =
                operations.preview(QUORUM, Operation.purge(), Scope.whole()).etag();
        assertThat(grown).isEqualTo(first);
        assertThat(moved).isNotEqualTo(first);
    }

    @Test
    void aMatchingEtagLetsAPurgeThrough() {
        get(QUEUE, queue("classic", 3, 0, "[]").replace("\"members\":[],", ""));
        ok("DELETE", QUEUE + "/contents");
        String etag =
                operations.preview(CLASSIC, Operation.purge(), Scope.whole()).etag();
        Partial<OperationResult> result =
                operations.execute(CLASSIC, Operation.purge(), Scope.whole(), Precondition.ifMatch(etag));
        assertThat(result.failures()).isEmpty();
        assertThat(result.value().affected()).hasValue(3);
        assertThat(writes())
                .extracting(FakeManagement.Received::method, FakeManagement.Received::path)
                .containsExactly(tuple("DELETE", QUEUE + "/contents"));
    }

    @Test
    void aStaleEtagMakesNoWriteAndSkipsTheNode() {
        get(QUEUE, queue("quorum", 1, 0, "[\"rabbit@a\"]"));
        get(QUEUE, queue("quorum", 1, 0, "[\"rabbit@a\",\"rabbit@b\"]"));
        ok("DELETE", QUEUE + "/contents");
        String etag =
                operations.preview(QUORUM, Operation.purge(), Scope.whole()).etag();
        Partial<OperationResult> result =
                operations.execute(QUORUM, Operation.purge(), Scope.whole(), Precondition.ifMatch(etag));
        BrokerException e = failure(result);
        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
        assertThat(e.reason()).isEqualTo(ErrorReasons.PRECONDITION_FAILED);
        assertThat(result.outcomes().values()).containsOnly(NodeOutcome.SKIPPED);
        assertThat(writes()).isEmpty();
    }

    @Test
    void aStreamPurgeIsRefusedWithoutAnyCall() {
        BrokerException e = failure(run(STREAM, Operation.purge()));
        assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_SUPPORTED);
        assertThatThrownBy(() -> operations.preview(STREAM, Operation.purge(), Scope.whole()))
                .isInstanceOf(BrokerException.class);
        assertThat(server.received()).isEmpty();
    }

    @Test
    void verbsTheKindsDoNotOfferAreRefusedWithoutAnyCall() {
        for (Operation operation : List.of(
                new Operation.Pause(Direction.BOTH),
                new Operation.Resume(Direction.BOTH),
                new Operation.Seek(Position.earliest()),
                new Operation.Truncate(Position.earliest()),
                new Operation.Reassign(Map.of()),
                new Operation.ElectLeader(Election.PREFERRED),
                new Operation.Resolve(Resolution.COMMIT),
                new Operation.Close(),
                Operation.purge())) {
            BrokerException e = failure(run(ResourceId.of("rabbitmq.exchange", "/", "x"), operation));
            assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_SUPPORTED);
        }
        assertThat(failure(run(ResourceId.of("rabbitmq.federation-link", "/", "u", "queue", "n"), Operation.delete()))
                        .reason())
                .isEqualTo(ErrorReasons.NOT_SUPPORTED);
        assertThat(server.received()).isEmpty();
    }

    @Test
    void aClassicQueueDeleteSendsTheConditionsAsFlags() {
        get(QUEUE, queue("classic", 0, 0, "[]"));
        ok("DELETE", QUEUE);
        assertThat(run(CLASSIC, new Operation.Delete(true, false)).failures()).isEmpty();
        FakeManagement.Received delete = writes().getFirst();
        assertThat(delete.query()).contains("if-empty=true").contains("if-unused=true");
    }

    @Test
    void aQuorumQueueDeleteChecksTheConditionsItself() {
        get(QUEUE, queue("quorum", 4, 0, "[\"rabbit@a\"]"));
        assertThat(failure(run(QUORUM, new Operation.Delete(true, true))).reason())
                .isEqualTo(ErrorReasons.NOT_EMPTY);
        get("/api/queues/%2F/busy", queue("stream", 0, 2, "[\"rabbit@a\"]"));
        ResourceId busy = ResourceId.of("rabbitmq.stream", "/", "busy");
        assertThat(failure(run(busy, new Operation.Delete(false, false))).reason())
                .isEqualTo(ErrorReasons.HAS_CONSUMERS);
        assertThat(writes()).isEmpty();
        ok("DELETE", QUEUE);
        assertThat(run(QUORUM, new Operation.Delete(false, true)).failures()).isEmpty();
        assertThat(writes().getFirst().query()).isNull();
    }

    @Test
    void aQueueIsCreatedWithItsTypeAndArguments() {
        ok("PUT", QUEUE);
        Attributes attributes = Attributes.builder()
                .putRaw("openbroker.durable", true)
                .putRaw("rabbitmq.delivery_limit", 5L)
                .putRaw("rabbitmq.argument.x-max-length", 100L)
                .build();
        assertThat(run(QUORUM, new Operation.Create(attributes)).failures()).isEmpty();
        JsonNode body = body(writes().getFirst());
        assertThat(body.path("durable").asBoolean()).isTrue();
        assertThat(body.path("arguments").path("x-queue-type").asString()).isEqualTo("quorum");
        assertThat(body.path("arguments").path("x-delivery-limit").asLong()).isEqualTo(5);
        assertThat(body.path("arguments").path("x-max-length").asLong()).isEqualTo(100);
    }

    @Test
    void streamArgumentsAreMapped() {
        ok("PUT", QUEUE);
        Attributes attributes = Attributes.builder()
                .putRaw("rabbitmq.max_age", "7D")
                .putRaw("rabbitmq.max_length_bytes", 1000L)
                .build();
        assertThat(run(STREAM, new Operation.Create(attributes)).failures()).isEmpty();
        JsonNode arguments = body(writes().getFirst()).path("arguments");
        assertThat(arguments.path("x-queue-type").asString()).isEqualTo("stream");
        assertThat(arguments.path("x-max-age").asString()).isEqualTo("7D");
        assertThat(arguments.path("x-max-length-bytes").asLong()).isEqualTo(1000);
    }

    @Test
    void anInequivalentQueueIsAlreadyThere() {
        server.respond(
                "PUT", QUEUE, 400, JSON, "{\"error\":\"bad_request\",\"reason\":\"inequivalent arg 'durable'\"}");
        BrokerException e = failure(run(CLASSIC, new Operation.Create(Attributes.empty())));
        assertThat(e.status()).isEqualTo(Status.ALREADY_EXISTS);
        assertThat(e.reason()).isEqualTo(ErrorReasons.RESOURCE_EXISTS);
    }

    @Test
    void anExchangeIsCreatedAndNeedsItsType() {
        ResourceId id = ResourceId.of("rabbitmq.exchange", "/", "events");
        ok("PUT", "/api/exchanges/%2F/events");
        assertThat(failure(run(id, new Operation.Create(Attributes.empty()))).status())
                .isEqualTo(Status.INVALID_ARGUMENT);
        Attributes attributes = Attributes.builder()
                .putRaw("rabbitmq.exchange_type", "topic")
                .putRaw("rabbitmq.internal", true)
                .putRaw("rabbitmq.argument.alternate-exchange", "other")
                .build();
        assertThat(run(id, new Operation.Create(attributes)).failures()).isEmpty();
        JsonNode body = body(writes().getFirst());
        assertThat(body.path("type").asString()).isEqualTo("topic");
        assertThat(body.path("internal").asBoolean()).isTrue();
        assertThat(body.path("arguments").path("alternate-exchange").asString()).isEqualTo("other");
    }

    @Test
    void anExchangeDeleteChecksBuiltInsAndBindings() {
        ResourceId builtIn = ResourceId.of("rabbitmq.exchange", "/", "amq.direct");
        get("/api/exchanges/%2F/amq.direct", "{\"type\":\"direct\",\"durable\":true}");
        assertThat(failure(run(builtIn, Operation.delete())).status()).isEqualTo(Status.FAILED_PRECONDITION);

        ResourceId events = ResourceId.of("rabbitmq.exchange", "/", "events");
        get("/api/exchanges/%2F/events", "{\"type\":\"topic\",\"durable\":true}");
        get("/api/exchanges/%2F/events/bindings/source", "[{\"destination\":\"orders\"}]");
        assertThat(failure(run(events, new Operation.Delete(true, false))).reason())
                .isEqualTo(ErrorReasons.HAS_CHILDREN);
        assertThat(writes()).isEmpty();

        ok("DELETE", "/api/exchanges/%2F/events");
        assertThat(run(events, new Operation.Delete(false, false)).failures()).isEmpty();
        assertThat(writes().getFirst().query()).isEqualTo("if-unused=false");
    }

    @Test
    void aBindingIsPostedAndDeletedAtItsPropertiesKey() {
        ResourceId binding = ResourceId.of("rabbitmq.binding", "/", "events", "orders", "key");
        server.respond("POST", "/api/bindings/%2F/e/events/q/orders", 201, Map.of("Location", "x/key"), "");
        Attributes attributes = Attributes.builder()
                .putRaw("openbroker.filter", "order.*")
                .putRaw("rabbitmq.argument.x-match", "all")
                .build();
        assertThat(run(binding, new Operation.Create(attributes)).failures()).isEmpty();
        JsonNode body = body(writes().getFirst());
        assertThat(body.path("routing_key").asString()).isEqualTo("order.*");
        assertThat(body.path("arguments").path("x-match").asString()).isEqualTo("all");

        get("/api/bindings/%2F/e/events/q/orders/key", "{\"routing_key\":\"order.*\",\"arguments\":{}}");
        ok("DELETE", "/api/bindings/%2F/e/events/q/orders/key");
        assertThat(run(binding, Operation.delete()).failures()).isEmpty();
        assertThat(writes().getLast().path()).isEqualTo("/api/bindings/%2F/e/events/q/orders/key");
    }

    @Test
    void anExchangeToExchangeBindingUsesTheDestinationExchange() {
        ResourceId binding = ResourceId.of("rabbitmq.exchange-binding", "/", "a", "b", "~");
        server.respond("POST", "/api/bindings/%2F/e/a/e/b", 201, Map.of(), "");
        assertThat(run(binding, new Operation.Create(Attributes.empty())).failures())
                .isEmpty();
        assertThat(body(writes().getFirst()).path("routing_key").asString()).isEmpty();
    }

    @Test
    void aPolicyIsCreatedThenUpdatedByMergingTheCurrentOne() {
        ResourceId id = ResourceId.of("rabbitmq.policy", "/", "ttl");
        ok("PUT", "/api/policies/%2F/ttl");
        Attributes create = Attributes.builder()
                .putRaw("rabbitmq.pattern", "^q")
                .putRaw("rabbitmq.priority", 2L)
                .putRaw("rabbitmq.definition.message-ttl", "60000")
                .putRaw("rabbitmq.definition.queue-mode", "lazy")
                .build();
        assertThat(run(id, new Operation.Create(create)).failures()).isEmpty();
        JsonNode created = body(writes().getFirst());
        assertThat(created.path("pattern").asString()).isEqualTo("^q");
        assertThat(created.path("apply-to").asString()).isEqualTo("all");
        assertThat(created.path("priority").asLong()).isEqualTo(2);
        assertThat(created.path("definition").path("message-ttl").asLong()).isEqualTo(60000);
        assertThat(created.path("definition").path("queue-mode").asString()).isEqualTo("lazy");

        get(
                "/api/policies/%2F/ttl",
                "{\"pattern\":\"^q\",\"apply-to\":\"queues\",\"priority\":2,\"definition\":{\"message-ttl\":60000}}");
        Attributes patch = Attributes.builder().putRaw("rabbitmq.priority", 5L).build();
        Preview preview = operations.preview(id, new Operation.Update(patch), Scope.whole());
        assertThat(preview.diff()).hasSize(1);
        assertThat(run(id, new Operation.Update(patch)).failures()).isEmpty();
        JsonNode updated = body(writes().getLast());
        assertThat(updated.path("priority").asLong()).isEqualTo(5);
        assertThat(updated.path("apply-to").asString()).isEqualTo("queues");
        assertThat(updated.path("definition").path("message-ttl").asLong()).isEqualTo(60000);
    }

    @Test
    void aPolicyNeedsAPatternAndAWholeNumberPriority() {
        ResourceId id = ResourceId.of("rabbitmq.operator-policy", "/", "p");
        assertThat(failure(run(id, new Operation.Create(Attributes.empty()))).status())
                .isEqualTo(Status.INVALID_ARGUMENT);
        Attributes bad = Attributes.builder()
                .putRaw("rabbitmq.pattern", ".*")
                .putRaw("rabbitmq.priority", "high")
                .build();
        assertThat(failure(run(id, new Operation.Create(bad))).status()).isEqualTo(Status.INVALID_ARGUMENT);
        assertThat(writes()).isEmpty();
    }

    @Test
    void anUpdateNeedsAPatchAndAnExistingObject() {
        ResourceId id = ResourceId.of("rabbitmq.policy", "/", "ttl");
        assertThat(failure(run(id, new Operation.Update(Attributes.empty()))).status())
                .isEqualTo(Status.INVALID_ARGUMENT);
        Attributes patch = Attributes.builder().putRaw("rabbitmq.priority", 1L).build();
        BrokerException gone = failure(run(id, new Operation.Update(patch)));
        assertThat(gone.reason()).isEqualTo(ErrorReasons.RESOURCE_GONE);
        assertThat(writes()).isEmpty();
    }

    @Test
    void permissionsAreSetUpdatedAndDeleted() {
        ResourceId id = ResourceId.of("rabbitmq.permission", "/", "app");
        ok("PUT", "/api/permissions/%2F/app");
        Attributes create = Attributes.builder()
                .putRaw("rabbitmq.configure", "^app")
                .putRaw("rabbitmq.read", ".*")
                .build();
        assertThat(run(id, new Operation.Create(create)).failures()).isEmpty();
        JsonNode body = body(writes().getFirst());
        assertThat(body.path("configure").asString()).isEqualTo("^app");
        assertThat(body.path("write").asString()).isEmpty();

        get("/api/permissions/%2F/app", "{\"configure\":\"^app\",\"write\":\"\",\"read\":\".*\"}");
        Attributes patch = Attributes.builder().putRaw("rabbitmq.write", "^app").build();
        assertThat(run(id, new Operation.Update(patch)).failures()).isEmpty();
        assertThat(body(writes().getLast()).path("write").asString()).isEqualTo("^app");

        ok("DELETE", "/api/permissions/%2F/app");
        assertThat(run(id, Operation.delete()).failures()).isEmpty();
    }

    @Test
    void aTopicPermissionIsFoundByItsExchangeAndDeletedOnlyWhenAlone() {
        ResourceId id = ResourceId.of("rabbitmq.topic-permission", "/", "app", "events");
        get("/api/topic-permissions/%2F/app", "[{\"exchange\":\"events\",\"write\":\"a\",\"read\":\"b\"}]");
        ok("PUT", "/api/topic-permissions/%2F/app");
        ok("DELETE", "/api/topic-permissions/%2F/app");
        Attributes patch = Attributes.builder().putRaw("rabbitmq.read", "c").build();
        assertThat(run(id, new Operation.Update(patch)).failures()).isEmpty();
        JsonNode body = body(writes().getFirst());
        assertThat(body.path("exchange").asString()).isEqualTo("events");
        assertThat(body.path("read").asString()).isEqualTo("c");
        assertThat(run(id, Operation.delete()).failures()).isEmpty();

        get(
                "/api/topic-permissions/%2F/two",
                "[{\"exchange\":\"events\",\"write\":\"a\",\"read\":\"b\"},{\"exchange\":\"x\",\"write\":\"\",\"read\":\"\"}]");
        ResourceId crowded = ResourceId.of("rabbitmq.topic-permission", "/", "two", "events");
        assertThat(failure(run(crowded, Operation.delete())).reason()).isEqualTo(ErrorReasons.NOT_SUPPORTED);
        ResourceId other = ResourceId.of("rabbitmq.topic-permission", "/", "two", "missing");
        assertThat(failure(run(other, Operation.delete())).reason()).isEqualTo(ErrorReasons.RESOURCE_GONE);
    }

    @Test
    void aVhostIsCreatedUpdatedAndDeletedWithItsContentsNamed() {
        ResourceId id = ResourceId.of("rabbitmq.vhost", "prod");
        ok("PUT", "/api/vhosts/prod");
        Attributes create = Attributes.builder()
                .putRaw("rabbitmq.description", "production")
                .putRaw("rabbitmq.tags", "a,b")
                .putRaw("rabbitmq.default_queue_type", "quorum")
                .build();
        assertThat(run(id, new Operation.Create(create)).failures()).isEmpty();
        JsonNode body = body(writes().getFirst());
        assertThat(body.path("description").asString()).isEqualTo("production");
        assertThat(body.path("tags").asString()).isEqualTo("a,b");
        assertThat(body.path("default_queue_type").asString()).isEqualTo("quorum");

        get("/api/vhosts/prod", "{\"name\":\"prod\",\"description\":\"production\",\"tags\":[\"a\",\"b\"]}");
        get("/api/queues/prod", "[{\"name\":\"q1\",\"messages\":4,\"consumers\":0},{\"name\":\"q2\",\"messages\":6}]");
        Preview preview = operations.preview(id, new Operation.Delete(false, false), Scope.whole());
        assertThat(preview.estimate().count()).isEqualTo(Count.estimate(10));
        assertThat(failure(run(id, new Operation.Delete(true, false))).reason()).isEqualTo(ErrorReasons.HAS_CHILDREN);
        ok("DELETE", "/api/vhosts/prod");
        assertThat(run(id, new Operation.Delete(false, false)).failures()).isEmpty();
        assertThat(writes().getLast().path()).isEqualTo("/api/vhosts/prod");
    }

    @Test
    void aPagedQueueListingIsReadToTheEnd() {
        ResourceId id = ResourceId.of("rabbitmq.vhost", "prod");
        get("/api/vhosts/prod", "{\"name\":\"prod\"}");
        get("/api/queues/prod", "{\"items\":[{\"name\":\"q\",\"messages\":2}],\"page\":1,\"page_count\":1}");
        Preview preview = operations.preview(id, Operation.delete(), Scope.whole());
        assertThat(preview.estimate().count()).isEqualTo(Count.estimate(2));
    }

    @Test
    void aConnectionIsClosed() {
        ResourceId id = ResourceId.of("rabbitmq.connection", "10.0.0.1:5000 -> 10.0.0.2:5672");
        String path = "/api/connections/10.0.0.1%3A5000%20-%3E%2010.0.0.2%3A5672";
        get(path, "{\"name\":\"c\",\"connected_at\":1700000000000}");
        ok("DELETE", path);
        assertThat(run(id, new Operation.Close()).failures()).isEmpty();
        assertThat(writes()).hasSize(1);
    }

    @Test
    void aShovelIsDeletedAndAStaticOneIsRefused() {
        ResourceId id = ResourceId.of("rabbitmq.shovel", "/", "move");
        get("/api/parameters/shovel/%2F/move", "{\"name\":\"move\",\"value\":{\"src-queue\":\"a\"}}");
        ok("DELETE", "/api/parameters/shovel/%2F/move");
        assertThat(run(id, Operation.delete()).failures()).isEmpty();

        ResourceId fixed = ResourceId.of("rabbitmq.shovel", "/", "fixed");
        get("/api/shovels/%2F", "[{\"name\":\"fixed\",\"type\":\"static\"}]");
        assertThat(failure(run(fixed, Operation.delete())).status()).isEqualTo(Status.FAILED_PRECONDITION);
        ResourceId missing = ResourceId.of("rabbitmq.shovel", "/", "none");
        assertThat(failure(run(missing, Operation.delete())).reason()).isEqualTo(ErrorReasons.RESOURCE_GONE);
        assertThat(writes()).hasSize(1);
    }

    @Test
    void aServerErrorOnAWriteIsInDoubt() {
        get(QUEUE, queue("classic", 0, 0, "[]"));
        server.respond("DELETE", QUEUE, 500, JSON, "{\"error\":\"x\",\"reason\":\"boom\"}");
        Partial<OperationResult> result = run(CLASSIC, Operation.delete());
        assertThat(result.outcomes().values()).containsOnly(NodeOutcome.IN_DOUBT);
    }

    @Test
    void aBadIdIsRefused() {
        ResourceId id = ResourceId.of("rabbitmq.classic-queue", "orders");
        assertThat(failure(run(id, Operation.purge())).status()).isEqualTo(Status.INVALID_ARGUMENT);
        ResourceId user = ResourceId.of("rabbitmq.user", "bob");
        assertThat(Writes.etag(user, Optional.empty())).isNotBlank();
        assertThatThrownBy(() -> Writes.objectPath(user)).isInstanceOf(BrokerException.class);
    }

    @Test
    void aBindingIsAddressedByItsPropertiesKeyEncodedOnce() {
        // The management API names a binding "order.%23" for the routing key "order.#" and "~" for an empty one.
        assertThat(Writes.objectPath(ResourceId.of("rabbitmq.binding", "/", "events", "orders", "order.%23")))
                .isEqualTo("/api/bindings/%2F/e/events/q/orders/order.%2523");
        assertThat(Writes.objectPath(ResourceId.of("rabbitmq.binding", "/", "events", "orders", "~")))
                .isEqualTo("/api/bindings/%2F/e/events/q/orders/~");
        assertThat(Writes.objectPath(ResourceId.of("rabbitmq.exchange-binding", "/", "a", "b", "~")))
                .isEqualTo("/api/bindings/%2F/e/a/e/b/~");
    }
}
