package io.github.sudoitir.openbroker.tck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.CallScope;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.AccessSnapshot;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.messaging.BrowsePage;
import io.github.sudoitir.openbroker.messaging.BrowseRequest;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageAction;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.Selection;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Total;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.Ordering;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Verb;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The conformance kit. Extend it in a driver's integration test and return a {@link TckEnvironment}
 * backed by a real broker; every case checks one promise of the SDK, and a case whose verb or
 * capability the driver does not declare is skipped, not failed.
 */
public abstract class OpenBrokerTck {

    private BrokerDrivers drivers;
    private ClusterConnection connection;
    private TckEnvironment env;

    /** The environment; called once per test. */
    protected abstract TckEnvironment environment();

    @BeforeEach
    void connect() {
        env = environment();
        drivers = BrokerDrivers.of(List.of(env.driver()));
        connection = drivers.connect(env.request());
    }

    @AfterEach
    void disconnect() {
        connection.close();
        drivers.close();
    }

    // Connection and nodes.

    @Test
    void describesEveryNodeOfTheCluster() {
        Partial<List<NodeDescription>> nodes = connection.nodes().describe(Scope.whole());
        assertThat(nodes.failures()).isEmpty();
        assertThat(nodes.value()).hasSize(env.request().cluster().nodes().size());
        assertThat(nodes.value()).allSatisfy(n -> assertThat(n.nativeId()).isNotBlank());
    }

    @Test
    void verifiesWithoutKeepingAConnection() {
        var report = drivers.verify(env.request());
        assertThat(report.nodes().failures()).isEmpty();
        assertThat(report.access().of(Capability.MANAGEMENT_READ).status()).isEqualTo(Assessment.Status.AVAILABLE);
    }

    @Test
    void aWrongPasswordFailsAsUnauthenticatedWithoutShowingIt() {
        String wrong = "tck-wrong-" + UUID.randomUUID();
        try (ClusterConnection bad = drivers.connect(env.requestWithWrongPassword(wrong))) {
            Partial<List<NodeDescription>> nodes = bad.nodes().describe(Scope.whole());
            assertThat(nodes.failures()).isNotEmpty();
            assertThat(nodes.failures().values()).allSatisfy(e -> {
                assertThat(e.status()).isIn(Status.UNAUTHENTICATED, Status.PERMISSION_DENIED);
                assertThat(e.reason()).isEqualTo(ErrorReasons.CREDENTIALS_REJECTED);
                assertThat(e.toString()).doesNotContain(wrong);
                assertThat(String.valueOf(e.getMessage())).doesNotContain(wrong);
                assertThat(e.info().metadata().values()).noneMatch(v -> v.contains(wrong));
            });
        } catch (BrokerException e) {
            assertThat(e.reason()).isEqualTo(ErrorReasons.CREDENTIALS_REJECTED);
            assertThat(e.toString()).doesNotContain(wrong);
        }
    }

    @Test
    void anExpiredDeadlineFailsAsDeadlineExceeded() throws Exception {
        Deadline past = new Deadline(Instant.now().minusSeconds(1));
        assertThatThrownBy(() -> ScopedValue.where(CallScope.DEADLINE, past)
                        .call(() -> connection
                                .resources()
                                .read(ResourceQuery.of(ResourceClass.SUBSCRIPTION))
                                .orThrow()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.DEADLINE_EXCEEDED));
    }

    @Test
    void theInterceptorAdmitsAndCompletesEveryCall() {
        AtomicInteger admitted = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        CallInterceptor counting = call -> {
            admitted.incrementAndGet();
            assertThat(call.cost()).isPositive();
            assertThat(call.operation()).isNotBlank();
            return new Permit() {
                @Override
                public void complete(Outcome outcome, Optional<Instant> serverTime) {
                    completed.incrementAndGet();
                }
            };
        };
        ConnectRequest r = env.request();
        try (ClusterConnection c = drivers.connect(new ConnectRequest(
                r.system(), r.cluster(), r.credentials(), r.options().withInterceptor(counting)))) {
            c.resources().read(ResourceQuery.of(ResourceClass.SUBSCRIPTION)).orThrow();
        }
        assertThat(admitted.get()).isPositive();
        assertThat(completed.get()).isEqualTo(admitted.get());
    }

    // Resources and kinds.

    @Test
    void everyKindDeclaresItsClassAndVerbs() {
        List<Kind> kinds = connection.resources().kinds();
        assertThat(kinds).isNotEmpty();
        assertThat(kinds).allSatisfy(k -> {
            assertThat(k.id().system()).isEqualTo(env.driver().system().id());
            assertThat(k.resourceClass()).isNotNull();
        });
        assertThat(kinds).anySatisfy(k -> assertThat(k.id()).isEqualTo(env.subscriptionKind()));
    }

    @Test
    void aCreatedSubscriptionIsListedWithItsClass() {
        ResourceId id = createSubscription();
        try {
            ResourcePage page = connection
                    .resources()
                    .read(ResourceQuery.of(env.subscriptionKind()).named(id.name()))
                    .orThrow();
            assertThat(page.items()).extracting(r -> r.id()).contains(id);
            assertThat(kind(env.subscriptionKind().value()).resourceClass()).isEqualTo(ResourceClass.SUBSCRIPTION);
        } finally {
            delete(id);
        }
    }

    @Test
    void anOrderingTheKindDoesNotDeclareIsRefused() {
        Kind k = kind(env.subscriptionKind().value());
        String undeclared = "tck-undeclared-field";
        assumeTrue(!k.orderings().contains(undeclared));
        assertThatThrownBy(() -> connection
                        .resources()
                        .read(ResourceQuery.of(k.id()).orderBy(List.of(Ordering.asc(undeclared))))
                        .orThrow())
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }

    // Operations.

    @Test
    void createIsIdempotent() {
        assumeVerb(Verb.CREATE);
        ResourceId id = createSubscription();
        try {
            Partial<OperationResult> again = connection
                    .operations()
                    .execute(id, new Operation.Create(env.subscriptionAttributes(id.name())), Scope.whole());
            assertThat(again.failures()).isEmpty();
        } finally {
            delete(id);
        }
    }

    @Test
    void aPurgeLeavesNoMessageBehind() {
        assumeVerb(Verb.PURGE);
        ResourceId id = createSubscription();
        try {
            send(id, 5, "k", "v");
            connection
                    .operations()
                    .execute(id, Operation.purge(), Scope.whole())
                    .orThrow();
            assertThat(exactCount(id)).isZero();
        } finally {
            delete(id);
        }
    }

    @Test
    void anEtagRefusesARecreatedResourceButNotAGrownBacklog() {
        assumeVerb(Verb.PURGE);
        ResourceId id = createSubscription();
        try {
            send(id, 2, "k", "v");
            Preview preview = connection.operations().preview(id, Operation.purge(), Scope.whole());
            assertThat(preview.etag()).isNotBlank();

            send(id, 3, "k", "v");
            Partial<OperationResult> grown = connection
                    .operations()
                    .execute(id, Operation.purge(), Scope.whole(), Precondition.ifMatch(preview.etag()));
            assertThat(grown.failures()).isEmpty();

            Preview second = connection.operations().preview(id, Operation.purge(), Scope.whole());
            delete(id);
            createSubscription(id);
            Partial<OperationResult> recreated = connection
                    .operations()
                    .execute(id, Operation.purge(), Scope.whole(), Precondition.ifMatch(second.etag()));
            assertThat(recreated.failures().values())
                    .isNotEmpty()
                    .allSatisfy(e -> assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION));
            assertThat(recreated.outcomes().values()).doesNotContain(NodeOutcome.APPLIED);
        } finally {
            delete(id);
        }
    }

    // Messages.

    @Test
    void aSentMessageIsBrowsedWithItsHeadersAndTypedProperties() {
        assumeVerb(Verb.BROWSE);
        ResourceId id = createSubscription();
        try {
            OutboundMessage out = new OutboundMessage(
                    MessageHeaders.builder()
                            .correlationId("tck-corr")
                            .type("tck.type")
                            .build(),
                    Map.of(
                            "text",
                            PropertyValue.of("hello"),
                            "number",
                            PropertyValue.of(42L),
                            "flag",
                            PropertyValue.of(true)),
                    MessageBody.of("tck-body".getBytes(StandardCharsets.UTF_8)),
                    Map.of());
            connection.messages().send(SendTarget.subscription(id), out);
            BrowsePage page = connection
                    .messages()
                    .browse(id, Scope.whole(), new BrowseRequest(Position.earliest(), 10, Optional.empty(), 1024));
            assertThat(page.messages()).hasSize(1);
            Message m = page.messages().getFirst();
            assertThat(m.headers().correlationId()).contains("tck-corr");
            assertThat(m.properties()).containsEntry("text", PropertyValue.of("hello"));
            assertThat(m.properties()).containsEntry("number", PropertyValue.of(42L));
            assertThat(m.properties()).containsEntry("flag", PropertyValue.of(true));
            assertThat(m.body().bytes())
                    .hasValueSatisfying(b -> assertThat(b.asUtf8()).isEqualTo("tck-body"));
        } finally {
            delete(id);
        }
    }

    @Test
    void aNativeFilterCountsOnlyWhatMatches() {
        assumeVerb(Verb.COUNT);
        ResourceId id = createSubscription();
        try {
            send(id, 3, "colour", "red");
            send(id, 2, "colour", "blue");
            Total total = connection
                    .messages()
                    .count(id, Scope.whole(), Optional.of(env.nativeEquals("colour", "red")))
                    .orThrow();
            assertThat(total.count()).isEqualTo(Count.exact(3));
        } finally {
            delete(id);
        }
    }

    @Test
    void aPortableFilterWorksOnEverySystem() {
        assumeVerb(Verb.COUNT);
        ResourceId id = createSubscription();
        try {
            send(id, 2, "colour", "red");
            send(id, 1, "colour", "blue");
            Total total = connection
                    .messages()
                    .count(id, Scope.whole(), Optional.of(MessageFilter.portable("property.colour = 'red'")))
                    .orThrow();
            assertThat(total.count()).isIn(Count.exact(2), Count.estimate(2));
        } finally {
            delete(id);
        }
    }

    @Test
    void aFilterThatEscapesItsBoundIsRefused() {
        assumeVerb(Verb.MOVE_MESSAGES);
        ResourceId id = createSubscription();
        try {
            Selection escape = new Selection.ByFilter(
                    new MessageFilter(env.nativeDialect(), "a=1) OR (1=1"), Optional.of(Instant.now()));
            assertThatThrownBy(() -> connection
                            .messages()
                            .bulk(id, Scope.whole(), escape, new MessageAction.Delete())
                            .orThrow())
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.INVALID_FILTER));
        } finally {
            delete(id);
        }
    }

    // Notifications.

    @Test
    void aSubscriptionStartsWithAGapAndSeesACreatedSubscription() throws Exception {
        assumeTrue(connection.notifications().isPresent(), "The driver offers no notifications");
        BlockingQueue<Object> received = new LinkedBlockingQueue<>();
        try (AutoCloseable subscription = connection
                .notifications()
                .get()
                .subscribe(Scope.whole(), new NotificationListener() {
                    @Override
                    public void onEvent(BrokerEvent event) {
                        received.add(event);
                    }

                    @Override
                    public void onGap(Gap gap) {
                        received.add(gap);
                    }
                })) {
            Object first = received.poll(30, TimeUnit.SECONDS);
            assertThat(first)
                    .isInstanceOfSatisfying(
                            Gap.class, g -> assertThat(g.cause()).isEqualTo(Gap.Cause.STARTED));
            ResourceId id = createSubscription();
            try {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                boolean seen = false;
                while (!seen && System.nanoTime() < until) {
                    Object next = received.poll(1, TimeUnit.SECONDS);
                    seen = next instanceof BrokerEvent e
                            && e.type().equals(EventTypes.RESOURCE_CREATED)
                            && e.subject().filter(id::equals).isPresent();
                }
                assertThat(seen).as("a created event about " + id).isTrue();
            } finally {
                delete(id);
            }
        }
    }

    // Access.

    @Test
    void theAccountsCapabilitiesAreAssessed() {
        AccessSnapshot snapshot = connection.access().probe();
        assertThat(snapshot.of(Capability.MANAGEMENT_READ).status()).isEqualTo(Assessment.Status.AVAILABLE);
        assertThat(snapshot.assessments().values())
                .allSatisfy(a -> assertThat(a.reason()).isNotBlank());
    }

    // Helpers.

    private Kind kind(String id) {
        return connection.resources().kinds().stream()
                .filter(k -> k.id().value().equals(id))
                .findFirst()
                .orElseThrow();
    }

    private void assumeVerb(Verb verb) {
        assumeTrue(kind(env.subscriptionKind().value()).supports(verb), "The driver does not declare " + verb);
    }

    private ResourceId createSubscription() {
        ResourceId id = env.subscriptionId("tck." + UUID.randomUUID().toString().substring(0, 8));
        createSubscription(id);
        return id;
    }

    private void createSubscription(ResourceId id) {
        connection
                .operations()
                .execute(id, new Operation.Create(env.subscriptionAttributes(id.name())), Scope.whole())
                .orThrow();
    }

    private void delete(ResourceId id) {
        try {
            connection.operations().execute(id, new Operation.Delete(false, true), Scope.whole());
        } catch (BrokerException ignored) {
            // Best effort: a case may already have deleted it.
        }
    }

    private void send(ResourceId id, int count, String property, String value) {
        for (int i = 0; i < count; i++) {
            connection
                    .messages()
                    .send(
                            SendTarget.subscription(id),
                            new OutboundMessage(
                                    MessageHeaders.empty(),
                                    Map.of(property, PropertyValue.of(value)),
                                    MessageBody.of(new byte[] {1}),
                                    Map.of()));
        }
    }

    private long exactCount(ResourceId id) {
        Total total =
                connection.messages().count(id, Scope.whole(), Optional.empty()).orThrow();
        return switch (total.count()) {
            case Count.Exact e -> e.value();
            case Count.Estimate e -> e.value();
            case Count.Unavailable u -> throw new AssertionError("Count unavailable: " + u.reason());
        };
    }
}
