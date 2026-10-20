package io.github.sudoitir.openbroker.artemis.internal;

import static io.github.sudoitir.openbroker.model.Verb.BROWSE;
import static io.github.sudoitir.openbroker.model.Verb.CLOSE;
import static io.github.sudoitir.openbroker.model.Verb.COPY_MESSAGES;
import static io.github.sudoitir.openbroker.model.Verb.COUNT;
import static io.github.sudoitir.openbroker.model.Verb.CREATE;
import static io.github.sudoitir.openbroker.model.Verb.DEAD_LETTER_MESSAGES;
import static io.github.sudoitir.openbroker.model.Verb.DELETE;
import static io.github.sudoitir.openbroker.model.Verb.DELETE_MESSAGES;
import static io.github.sudoitir.openbroker.model.Verb.GET;
import static io.github.sudoitir.openbroker.model.Verb.LIST;
import static io.github.sudoitir.openbroker.model.Verb.MOVE_MESSAGES;
import static io.github.sudoitir.openbroker.model.Verb.PAUSE;
import static io.github.sudoitir.openbroker.model.Verb.PURGE;
import static io.github.sudoitir.openbroker.model.Verb.RECEIVE;
import static io.github.sudoitir.openbroker.model.Verb.RESUME;
import static io.github.sudoitir.openbroker.model.Verb.RETRY_MESSAGES;
import static io.github.sudoitir.openbroker.model.Verb.SEND;
import static io.github.sudoitir.openbroker.model.Verb.UPDATE;

import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.model.AttributeSchema;
import io.github.sudoitir.openbroker.model.AttributeSpec;
import io.github.sudoitir.openbroker.model.Instrument;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.model.Verb;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Every kind the driver offers, with its verbs and attributes. Artemis declares no list ordering. */
public final class Catalog {

    private static final String MESSAGE = "{message}";

    public static final List<Kind> KINDS = List.of(
            kind(
                    ArtemisKinds.ADDRESS,
                    ResourceClass.DESTINATION,
                    Set.of(LIST, GET, CREATE, UPDATE, DELETE, SEND, CLOSE),
                    Optional.empty(),
                    List.of(AttributeSpec.config(ArtemisKinds.ROUTING_TYPE, true, false))),
            kind(
                    ArtemisKinds.QUEUE,
                    ResourceClass.SUBSCRIPTION,
                    Set.of(
                            LIST,
                            GET,
                            CREATE,
                            UPDATE,
                            DELETE,
                            PAUSE,
                            RESUME,
                            PURGE,
                            BROWSE,
                            COUNT,
                            SEND,
                            RECEIVE,
                            MOVE_MESSAGES,
                            COPY_MESSAGES,
                            DELETE_MESSAGES,
                            RETRY_MESSAGES,
                            DEAD_LETTER_MESSAGES),
                    Optional.of(ArtemisKinds.ADDRESS),
                    List.of(
                            AttributeSpec.config(StandardAttributes.DESTINATION.name(), false, true),
                            AttributeSpec.config(ArtemisKinds.ROUTING_TYPE, false, true),
                            AttributeSpec.config(StandardAttributes.DURABLE.name(), false, true),
                            AttributeSpec.config(StandardAttributes.FILTER.name(), true, false),
                            AttributeSpec.config(StandardAttributes.MAX_CONSUMERS.name(), true, false),
                            AttributeSpec.status(StandardAttributes.PAUSED.name()),
                            AttributeSpec.metric(MetricKeys.BACKLOG.name(), Instrument.UP_DOWN_COUNTER, MESSAGE),
                            AttributeSpec.metric(
                                    MetricKeys.CONSUMER_COUNT.name(), Instrument.UP_DOWN_COUNTER, "{consumer}"),
                            AttributeSpec.metric(MetricKeys.IN_FLIGHT.name(), Instrument.UP_DOWN_COUNTER, MESSAGE),
                            AttributeSpec.metric(MetricKeys.SCHEDULED.name(), Instrument.UP_DOWN_COUNTER, MESSAGE),
                            AttributeSpec.metric(MetricKeys.ENQUEUED.name(), Instrument.COUNTER, MESSAGE),
                            AttributeSpec.metric(MetricKeys.ACKNOWLEDGED.name(), Instrument.COUNTER, MESSAGE),
                            AttributeSpec.metric(MetricKeys.EXPIRED.name(), Instrument.COUNTER, MESSAGE))),
            kind(
                    ArtemisKinds.DIVERT,
                    ResourceClass.ROUTE,
                    Set.of(LIST, GET, CREATE, DELETE),
                    Optional.of(ArtemisKinds.ADDRESS),
                    List.of()),
            kind(ArtemisKinds.BRIDGE, ResourceClass.ROUTE, Set.of(LIST, GET), Optional.empty(), List.of()),
            kind(ArtemisKinds.CONNECTION, ResourceClass.CONNECTION, Set.of(LIST, CLOSE), Optional.empty(), List.of()),
            kind(
                    ArtemisKinds.SESSION,
                    ResourceClass.CHANNEL,
                    Set.of(LIST, CLOSE),
                    Optional.of(ArtemisKinds.CONNECTION),
                    List.of()),
            kind(
                    ArtemisKinds.CONSUMER,
                    ResourceClass.CONSUMER,
                    Set.of(LIST, CLOSE),
                    Optional.of(ArtemisKinds.SESSION),
                    List.of()),
            kind(
                    ArtemisKinds.PRODUCER,
                    ResourceClass.PRODUCER,
                    Set.of(LIST),
                    Optional.of(ArtemisKinds.SESSION),
                    List.of()),
            kind(
                    ArtemisKinds.ADDRESS_SETTING,
                    ResourceClass.POLICY,
                    Set.of(GET, CREATE, DELETE),
                    Optional.empty(),
                    List.of()),
            kind(
                    ArtemisKinds.SECURITY_SETTING,
                    ResourceClass.ACCESS_RULE,
                    Set.of(GET, CREATE, DELETE),
                    Optional.empty(),
                    List.of()),
            kind(ArtemisKinds.USER, ResourceClass.PRINCIPAL, Set.of(GET), Optional.empty(), List.of()));

    private Catalog() {}

    public static Kind kind(KindId id) {
        return KINDS.stream()
                .filter(k -> k.id().equals(id))
                .findFirst()
                .orElseThrow(() -> Queries.invalid("Artemis has no kind " + id + "."));
    }

    public static boolean supports(KindId id, Verb verb) {
        return kind(id).supports(verb);
    }

    private static Kind kind(
            KindId id, ResourceClass rc, Set<Verb> verbs, Optional<KindId> parent, List<AttributeSpec> attributes) {
        return new Kind(id, rc, verbs, parent, new AttributeSchema(attributes), Set.of());
    }
}
