package io.github.sudoitir.openbroker.artemis;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** The public helpers an adopter of the Jolokia plane uses: requests, responses, lists and refusals. */
class ArtemisSurfaceTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static JolokiaResponse parse(String json) {
        return JSON.readValue(json, JolokiaResponse.class);
    }

    @Test
    void aRequestNeverPrintsTheArgumentsAnExecCarries() {
        JolokiaRequest send = JolokiaRequest.exec(
                "org.apache.activemq.artemis:broker=\"b\"", "sendMessage", "body", "admin", "s3cret");

        assertThat(send.toString())
                .doesNotContain("s3cret")
                .doesNotContain("admin")
                .contains("3 withheld");
    }

    @Test
    void anAttributeIsReadFromTheMapShapeOfAListRead() {
        assertThat(parse("{\"status\":200,\"value\":{\"MessageCount\":3}}")
                        .attribute("MessageCount")
                        .asLong())
                .isEqualTo(3);
    }

    @Test
    void anAttributeIsReadFromTheScalarShape() {
        assertThat(parse("{\"status\":200,\"value\":3}")
                        .attribute("MessageCount")
                        .asLong())
                .isEqualTo(3);
    }

    @Test
    void anObjectValuedAttributeIsUnwrapped() {
        // An acceptor's Parameters is itself an object: the key tells the wrapper from the payload.
        JolokiaResponse response = parse("{\"status\":200,\"value\":{\"Parameters\":{\"protocols\":\"CORE\"}}}");

        assertThat(response.attribute("Parameters").get("protocols").asString()).isEqualTo("CORE");
    }

    @Test
    void anAbsentValueStaysAbsent() {
        assertThat(parse("{\"status\":200}").attribute("MessageCount")).isNull();
    }

    @Test
    void readAllReadsInBatchesOfTwoHundredAndKeepsEveryAnswer() {
        RecordingJolokia jolokia = new RecordingJolokia();
        List<String> names =
                IntStream.range(0, 450).mapToObj(i -> "bridge-" + i).toList();

        List<String> rows =
                ArtemisLists.readAll(jolokia, names, node -> node.get("Name").asString());

        assertThat(rows).hasSize(450);
        assertThat(jolokia.batches).containsExactly(200, 200, 50);
    }

    @Test
    void readAllOfNothingSendsNothing() {
        RecordingJolokia jolokia = new RecordingJolokia();

        assertThat(ArtemisLists.readAll(jolokia, List.of(), node -> node)).isEmpty();
        assertThat(jolokia.batches).isEmpty();
    }

    private static BrokerException refusal(String error) {
        return ArtemisErrors.refusal(new JolokiaResponse(500, null, error, null, null), "destroyQueue")
                .orElseThrow();
    }

    @Test
    void aQueueNoLongerOnTheNodeIsGoneRatherThanAJmxClassName() {
        BrokerException e = refusal("javax.management.InstanceNotFoundException :"
                + " org.apache.activemq.artemis:broker=\"primary\",component=addresses,address=\"SCRATCH\","
                + "subcomponent=queues,routing-type=\"anycast\",queue=\"SCRATCH\"");

        assertThat(e.status()).isEqualTo(Status.NOT_FOUND);
        assertThat(e.reason()).isEqualTo(ErrorReasons.RESOURCE_GONE);
        assertThat(e.getMessage()).contains("no longer on this node");
        assertThat(e.info().metadata()).containsEntry(ArtemisErrors.NATIVE_CODE, "InstanceNotFoundException");
    }

    @Test
    void aQueueWithConsumersIsItsOwnRefusal() {
        BrokerException e = refusal(
                "java.lang.IllegalStateException : AMQ229025: Cannot delete queue DST"
                        + " on binding DST - it has consumers = org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding");

        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
        assertThat(e.reason()).isEqualTo(ErrorReasons.HAS_CONSUMERS);
        assertThat(e.info().metadata()).containsEntry(ArtemisErrors.NATIVE_CODE, "AMQ229025");
    }

    @Test
    void anExistingQueueIsAlreadyExists() {
        BrokerException e = refusal(
                "ActiveMQQueueExistsException[errorType=QUEUE_EXISTS message=AMQ229019: Queue Q already exists]");

        assertThat(e.status()).isEqualTo(Status.ALREADY_EXISTS);
        assertThat(e.reason()).isEqualTo(ErrorReasons.RESOURCE_EXISTS);
    }

    @Test
    void anUnparsableAddressSettingNamesTheLastKeyTheBrokerQuoted() {
        BrokerException e = refusal("Error while parsing MetaData: name='max-size-bytes' then name='page-size-bytes'");

        assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
        assertThat(e.info().metadata()).containsEntry(ArtemisErrors.SETTING, "page-size-bytes");
    }

    @Test
    void anUnknownRefusalIsABadResponseNamingTheOperation() {
        BrokerException e = refusal("something else");

        assertThat(e.reason()).isEqualTo(ErrorReasons.BAD_RESPONSE);
        assertThat(e.getMessage()).startsWith("destroyQueue failed: something else");
    }

    @Test
    void aSuccessIsNoRefusal() {
        assertThat(ArtemisErrors.refusal(parse("{\"status\":200,\"value\":1}"), "x"))
                .isEmpty();
    }

    /** Answers every batch entry with a row and records each batch's size. */
    private static final class RecordingJolokia implements ArtemisJolokia {

        final List<Integer> batches = new ArrayList<>();

        @Override
        public List<JolokiaResponse> batch(List<JolokiaRequest> requests) {
            batches.add(requests.size());
            return requests.stream()
                    .map(r -> new JolokiaResponse(200, JSON.createObjectNode().put("Name", "x"), null, null, null))
                    .toList();
        }

        @Override
        public NodeId node() {
            return NodeId.of("n");
        }

        @Override
        public String brokerName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public JolokiaResponse single(JolokiaRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> search(String pattern) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JolokiaResponse readBroker(String... attributes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JolokiaResponse execOnBroker(String operation, Object... arguments) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable JsonNode execOnBrokerParsed(String operation, Object... arguments) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable JsonNode parsed(JolokiaResponse entry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ObjectMapper mapper() {
            return JSON;
        }
    }
}
