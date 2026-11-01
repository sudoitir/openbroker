package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.EtagBasis;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMq;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqTestCluster;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The operations facet against a real three-node cluster: what it sends, and what the server then holds. */
class RabbitMqOperationsIT {

    private static RabbitMqCluster cluster;
    private static RabbitMqOperations operations;
    private static RabbitMq admin;

    @BeforeAll
    static void connect() {
        RabbitMqTestCluster rabbit = RabbitMqTestCluster.shared();
        cluster = new RabbitMqCluster(
                rabbit.spec(),
                CredentialSource.of(rabbit.credentials()),
                new FakeManagement.TestContext(ConnectOptions.defaults()));
        operations = new RabbitMqOperations(cluster);
        admin = RabbitMq.of(rabbit, 0);
    }

    @AfterAll
    static void close() {
        cluster.close();
    }

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static Partial<OperationResult> run(ResourceId id, Operation operation) {
        return operations.execute(id, operation, Scope.whole());
    }

    @Test
    void aQueueOfEachTypeIsCreatedTwiceAndDeleted() {
        for (String kind : new String[] {"classic-queue", "quorum-queue", "stream"}) {
            ResourceId id = ResourceId.of("rabbitmq." + kind, "/", unique(kind));
            Operation create = new Operation.Create(Attributes.empty());
            assertThat(run(id, create).failures()).isEmpty();
            assertThat(run(id, create).failures()).isEmpty();
            assertThat(admin.exists("/api/queues/%2F/" + RabbitMq.segment(id.name())))
                    .isTrue();
            assertThat(run(id, Operation.delete()).failures()).isEmpty();
            assertThat(admin.exists("/api/queues/%2F/" + RabbitMq.segment(id.name())))
                    .isFalse();
        }
    }

    @Test
    void aPurgeEmptiesAQueueAndAStaleEtagLeavesItAlone() {
        String name = unique("purge");
        admin.declareQueue("/", name, "quorum");
        ResourceId id = ResourceId.of("rabbitmq.quorum-queue", "/", name);
        try {
            admin.publish("/", "", name, "one");
            RabbitMq.eventually(Duration.ofSeconds(30), () -> admin.messagesReady("/", name), n -> n == 1);
            Preview preview = operations.preview(id, Operation.purge(), Scope.whole());
            assertThat(preview.basis()).isEqualTo(EtagBasis.CONFIGURATION);

            admin.deleteQueue("/", name);
            admin.declareQueue("/", name, "quorum", Map.of("x-max-length", 50));
            Partial<OperationResult> stale =
                    operations.execute(id, Operation.purge(), Scope.whole(), Precondition.ifMatch(preview.etag()));
            assertThat(stale.failures().values()).allSatisfy(e -> {
                assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                assertThat(e.reason()).isEqualTo(ErrorReasons.PRECONDITION_FAILED);
            });
            assertThat(stale.outcomes().values()).doesNotContain(NodeOutcome.APPLIED);

            Preview fresh = operations.preview(id, Operation.purge(), Scope.whole());
            assertThat(operations
                            .execute(id, Operation.purge(), Scope.whole(), Precondition.ifMatch(fresh.etag()))
                            .failures())
                    .isEmpty();
        } finally {
            admin.deleteQueue("/", name);
        }
    }

    @Test
    void aStreamIsNotPurged() {
        String name = unique("log");
        admin.declareQueue("/", name, "stream");
        try {
            Partial<OperationResult> result = run(ResourceId.of("rabbitmq.stream", "/", name), Operation.purge());
            assertThat(result.failures().values())
                    .allSatisfy(e -> assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_SUPPORTED));
        } finally {
            admin.deleteQueue("/", name);
        }
    }

    @Test
    void aQuorumQueueWithMessagesRefusesAnIfEmptyDelete() {
        String name = unique("full");
        admin.declareQueue("/", name, "quorum");
        try {
            admin.publish("/", "", name, "one");
            RabbitMq.eventually(Duration.ofSeconds(30), () -> admin.messagesReady("/", name), n -> n == 1);
            Partial<OperationResult> refused =
                    run(ResourceId.of("rabbitmq.quorum-queue", "/", name), new Operation.Delete(true, false));
            assertThat(refused.failures().values())
                    .allSatisfy(e -> assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_EMPTY));
            assertThat(admin.exists("/api/queues/%2F/" + RabbitMq.segment(name)))
                    .isTrue();
        } finally {
            admin.deleteQueue("/", name);
        }
    }

    @Test
    void exchangesBindingsPoliciesPermissionsAndVhostsAreWrittenAndRemoved() {
        String vhost = unique("vh");
        ResourceId vhostId = ResourceId.of("rabbitmq.vhost", vhost);
        assertThat(run(
                                vhostId,
                                new Operation.Create(Attributes.builder()
                                        .putRaw("rabbitmq.description", "ops test")
                                        .build()))
                        .failures())
                .isEmpty();
        try {
            ResourceId exchange = ResourceId.of("rabbitmq.exchange", vhost, "events");
            assertThat(run(
                                    exchange,
                                    new Operation.Create(Attributes.builder()
                                            .putRaw("rabbitmq.exchange_type", "topic")
                                            .build()))
                            .failures())
                    .isEmpty();
            ResourceId queue = ResourceId.of("rabbitmq.classic-queue", vhost, "orders");
            assertThat(run(queue, new Operation.Create(Attributes.empty())).failures())
                    .isEmpty();
            ResourceId binding = ResourceId.of("rabbitmq.binding", vhost, "events", "orders", "order.%23");
            assertThat(run(
                                    binding,
                                    new Operation.Create(Attributes.builder()
                                            .putRaw("openbroker.filter", "order.#")
                                            .build()))
                            .failures())
                    .isEmpty();
            assertThat(admin.get("/api/exchanges/" + RabbitMq.segment(vhost) + "/events/bindings/source")
                            .size())
                    .isEqualTo(1);

            ResourceId policy = ResourceId.of("rabbitmq.policy", vhost, "limit");
            assertThat(run(
                                    policy,
                                    new Operation.Create(Attributes.builder()
                                            .putRaw("rabbitmq.pattern", "^orders")
                                            .putRaw("rabbitmq.apply_to", "queues")
                                            .putRaw("rabbitmq.definition.max-length", "10")
                                            .build()))
                            .failures())
                    .isEmpty();
            assertThat(run(
                                    policy,
                                    new Operation.Update(Attributes.builder()
                                            .putRaw("rabbitmq.priority", 3L)
                                            .build()))
                            .failures())
                    .isEmpty();
            assertThat(admin.get("/api/policies/" + RabbitMq.segment(vhost) + "/limit")
                            .path("priority")
                            .asInt())
                    .isEqualTo(3);

            assertThat(run(binding, Operation.delete()).failures()).isEmpty();

            ResourceId permission = ResourceId.of("rabbitmq.permission", vhost, RabbitMqTestCluster.ADMIN_USER);
            assertThat(run(
                                    permission,
                                    new Operation.Create(Attributes.builder()
                                            .putRaw("rabbitmq.configure", ".*")
                                            .putRaw("rabbitmq.write", ".*")
                                            .putRaw("rabbitmq.read", ".*")
                                            .build()))
                            .failures())
                    .isEmpty();
            assertThat(run(exchange, new Operation.Delete(true, false)).failures())
                    .isEmpty();
            assertThat(run(policy, Operation.delete()).failures()).isEmpty();
            // Last: the permission is the account's own on this virtual host, so nothing is writable after it.
            assertThat(run(permission, Operation.delete()).failures()).isEmpty();
        } finally {
            assertThat(run(vhostId, Operation.delete()).failures()).isEmpty();
        }
        assertThat(admin.exists("/api/vhosts/" + RabbitMq.segment(vhost))).isFalse();
    }
}
