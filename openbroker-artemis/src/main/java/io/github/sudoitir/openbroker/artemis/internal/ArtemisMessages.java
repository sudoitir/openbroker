package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.artemis.ArtemisMBeans;
import io.github.sudoitir.openbroker.artemis.JolokiaRequest;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import io.github.sudoitir.openbroker.messaging.BrowsePage;
import io.github.sudoitir.openbroker.messaging.BrowseRequest;
import io.github.sudoitir.openbroker.messaging.BulkOutcome;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.messaging.Denominator;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageAction;
import io.github.sudoitir.openbroker.messaging.MessageField;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.messaging.Selection;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Total;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * Messages over each node's Jolokia agent. A queue exists per node, so browse reads one node and
 * every other call runs on each node of its scope. Each mutating call is one operation the broker
 * counts itself; by id, the ids go in batches of {@value #BY_ID_BATCH}, and a run that stops part-way
 * reports which ids it did not act on.
 */
public final class ArtemisMessages implements Messages {

    /** The broker returns at most this many rows per browse page. */
    static final int PAGE_CAP = 200;

    /** Ids acted on per batch request, charged one permit. */
    static final int BY_ID_BATCH = 50;

    private static final String CURSOR_PREFIX = "artemis.page:";

    /** How long a browse waits for the first message of a page. */
    private static final Duration FIRST_WAIT = Duration.ofSeconds(1);

    private static final String MESSAGE_COUNT = "MessageCount";

    /** Artemis message types: the broker writes text for 3 and base64-decodes the body for 4. */
    private static final int TEXT = 3;

    private static final int BYTES = 4;

    /**
     * The overload with {@code createMessageId} (2.33 and later): without it a message sent over
     * Jolokia carries no message id, unlike one sent over Core.
     */
    private static final String SEND =
            "sendMessage(java.util.Map,int,java.lang.String,boolean,java.lang.String,java.lang.String,boolean)";

    private final Cluster cluster;
    private final CoreLinks core;

    ArtemisMessages(Cluster cluster, CoreLinks core) {
        this.cluster = cluster;
        this.core = core;
    }

    // ---- browse -------------------------------------------------------------

    @Override
    public BrowsePage browse(ResourceId subscription, Scope scope, BrowseRequest request) {
        requireQueue(subscription);
        NodeHandle node = oneNode(scope, "Browse reads one node; scope it to one node instance.");
        Deadline deadline = Deadlines.current(cluster.options());
        String selector = Selectors.of(request.filter());
        int size = Math.min(request.limit(), PAGE_CAP);
        int page =
                switch (request.position()) {
                    case Position.Earliest e -> 1;
                    case Position.After a -> page(a.cursor());
                    case Position.Latest l -> 0;
                    case Position.AtOffsets o -> page(o, node, size);
                    default -> throw Unimplemented.of("browse an Artemis queue from " + request.position());
                };
        if (page > 0 && node.core().isPresent() && (long) page * size <= PAGE_CAP) {
            Optional<BrowsePage> overCore = browseOverCore(node, subscription, selector, page, size, request, deadline);
            if (overCore.isPresent()) {
                return overCore.get();
            }
        }
        return retryOnceIfGone(node, subscription, () -> {
            String mbean = cluster.queueMBean(node, subscription.name(), deadline);
            List<JolokiaResponse> responses = cluster.jolokia(node)
                    .batch(
                            List.of(
                                    JolokiaRequest.exec(
                                            mbean,
                                            "browse(int,int,java.lang.String)",
                                            Math.max(page, 1),
                                            size,
                                            selector),
                                    JolokiaRequest.read(mbean, MESSAGE_COUNT),
                                    addressSettings(mbean)),
                            deadline,
                            Optional.of(subscription));
            JolokiaResponse browsed = responses.getFirst();
            requireOk(browsed, node, "browse");
            int sizeLimit = attributeSizeLimit(responses.size() > 2 ? responses.get(2) : null);
            List<Message> messages = new ArrayList<>();
            if (page > 0 && browsed.value() != null && browsed.value().isArray()) {
                for (JsonNode row : browsed.value()) {
                    messages.add(BrowseRows.parse(row, subscription, node.id(), sizeLimit, request.maxBodyBytes()));
                }
            }
            JsonNode count = responses.size() > 1 && responses.get(1).ok()
                    ? responses.get(1).attribute(MESSAGE_COUNT)
                    : null;
            // The count is the whole queue's, with or without a filter.
            Total total = new Total(
                    Denominator.ALL,
                    count != null && count.isNumber()
                            ? Count.exact(count.asLong())
                            : Count.unavailable("The broker did not return this queue's message count."));
            int rows = browsed.value() != null && browsed.value().isArray()
                    ? browsed.value().size()
                    : 0;
            boolean more = page > 0
                    && rows == size
                    && (!(total.count() instanceof Count.Exact exact) || (long) page * size < exact.value());
            return new BrowsePage(
                    messages, more ? Optional.of(new Cursor(CURSOR_PREFIX + (page + 1))) : Optional.empty(), total);
        });
    }

    /**
     * A page read over Core, which carries whole bodies and typed properties, or empty when Core
     * could not serve it and management should. A browser has no offset, so only the first window
     * of a queue is read this way. A browser also stops at the first message not yet on the client,
     * part-way through a paging queue, so a page shorter than the broker says is visible is read
     * again over management.
     */
    private Optional<BrowsePage> browseOverCore(
            NodeHandle node,
            ResourceId subscription,
            String selector,
            int page,
            int size,
            BrowseRequest request,
            Deadline deadline) {
        long skip = (long) (page - 1) * size;
        List<Message> rows;
        try {
            String destination = Destinations.of(cluster, node, subscription, deadline);
            rows = core.call(node, "browse", Optional.of(subscription), deadline, session -> {
                List<Message> out = new ArrayList<>(size);
                try (ClientConsumer browser = session.createConsumer(
                        SimpleString.of(destination), selector.isEmpty() ? null : SimpleString.of(selector), true)) {
                    long index = 0;
                    // The first message may take a moment to arrive; the rest are already buffered or close.
                    ClientMessage m = browser.receive(FIRST_WAIT.toMillis());
                    while (m != null && out.size() < size) {
                        if (index++ >= skip) {
                            out.add(read(m, subscription, node, request.maxBodyBytes()));
                        }
                        m = browser.receiveImmediate();
                    }
                }
                return out;
            });
        } catch (BrokerException e) {
            if (ErrorReasons.INVALID_FILTER.equals(e.reason())) {
                throw e;
            }
            return Optional.empty();
        }
        Optional<long[]> depth =
                node.jolokia().isPresent() ? visible(node, subscription, selector, deadline) : Optional.empty();
        if (depth.isPresent() && rows.size() < Math.min(size, depth.get()[1] - skip)) {
            return Optional.empty();
        }
        Total total = new Total(
                Denominator.ALL,
                depth.isPresent() && selector.isEmpty()
                        ? Count.exact(depth.get()[0])
                        : Count.unavailable("The broker did not return this queue's message count."));
        boolean more = depth.isPresent() && rows.size() == size && skip + size < depth.get()[1];
        return Optional.of(new BrowsePage(
                rows, more ? Optional.of(new Cursor(CURSOR_PREFIX + (page + 1))) : Optional.empty(), total));
    }

    /**
     * The queue's message count and the part of it a browser can see (less what is delivering and
     * what is scheduled), or with a filter the matching count for both; empty when unreadable.
     */
    private Optional<long[]> visible(NodeHandle node, ResourceId subscription, String selector, Deadline deadline) {
        try {
            String mbean = cluster.queueMBean(node, subscription.name(), deadline);
            JolokiaClient client = cluster.jolokia(node);
            if (selector.isEmpty()) {
                JolokiaResponse res = client.single(
                        JolokiaRequest.read(mbean, MESSAGE_COUNT, "DeliveringCount", "ScheduledCount"),
                        deadline,
                        Optional.of(subscription));
                if (!res.ok() || res.value() == null) {
                    return Optional.empty();
                }
                long count = res.value().path(MESSAGE_COUNT).asLong();
                return Optional.of(new long[] {
                    count,
                    count
                            - res.value().path("DeliveringCount").asLong()
                            - res.value().path("ScheduledCount").asLong()
                });
            }
            JolokiaResponse res = client.single(
                    JolokiaRequest.exec(mbean, "countMessages(java.lang.String)", selector),
                    deadline,
                    Optional.of(subscription));
            if (!res.ok() || res.value() == null) {
                return Optional.empty();
            }
            return Optional.of(new long[] {res.value().asLong(), res.value().asLong()});
        } catch (BrokerException e) {
            return Optional.empty();
        }
    }

    private static Message read(ClientMessage m, ResourceId subscription, NodeHandle node, long maxBodyBytes)
            throws ActiveMQException {
        try {
            return CoreBodies.read(m, subscription, node.id(), maxBodyBytes, Optional.empty());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The page an offset starts: the broker browses by page, so the offset into the node's queue is a
     * multiple of the limit.
     */
    private static int page(Position.AtOffsets at, NodeHandle node, int size) {
        Long offset = at.offsets().get(Shard.node(node.id()));
        if (offset == null || offset < 0 || offset % size != 0) {
            throw Queries.invalid(
                    "Browse an Artemis queue from an offset of this node that is a multiple of the limit.");
        }
        long page = offset / size + 1;
        if (page > Integer.MAX_VALUE) {
            throw Queries.invalid("That offset is past any queue Artemis can browse.");
        }
        return (int) page;
    }

    private static int page(Cursor cursor) {
        if (!cursor.token().startsWith(CURSOR_PREFIX)) {
            throw Queries.invalid("Not an Artemis browse cursor.");
        }
        try {
            int page = Integer.parseInt(cursor.token().substring(CURSOR_PREFIX.length()));
            if (page < 1) {
                throw Queries.invalid("Not an Artemis browse cursor.");
            }
            return page;
        } catch (NumberFormatException e) {
            throw Queries.invalid("Not an Artemis browse cursor.");
        }
    }

    /** The queue's address settings, read on the broker MBean its object name names. */
    private static JolokiaRequest addressSettings(String queueMbean) {
        try {
            ObjectName queue = ObjectName.getInstance(queueMbean);
            String broker = queue.getDomain() + ":broker=" + queue.getKeyProperty("broker");
            return JolokiaRequest.exec(
                    broker,
                    "getAddressSettingsAsJSON(java.lang.String)",
                    ObjectName.unquote(queue.getKeyProperty("address")));
        } catch (MalformedObjectNameException | RuntimeException e) {
            throw new IllegalStateException("Not a queue MBean: " + queueMbean, e);
        }
    }

    private int attributeSizeLimit(@Nullable JolokiaResponse settings) {
        if (settings == null || !settings.ok()) {
            return BrowseRows.DEFAULT_ATTRIBUTE_SIZE_LIMIT;
        }
        try {
            JsonNode limit = settings.valueParsed(cluster.mapper()).get("managementMessageAttributeSizeLimit");
            return limit != null && limit.isNumber() ? limit.asInt() : BrowseRows.DEFAULT_ATTRIBUTE_SIZE_LIMIT;
        } catch (RuntimeException e) {
            return BrowseRows.DEFAULT_ATTRIBUTE_SIZE_LIMIT;
        }
    }

    // ---- counts -------------------------------------------------------------

    @Override
    public Partial<Total> count(ResourceId subscription, Scope scope, Optional<MessageFilter> filter) {
        requireQueue(subscription);
        String selector = Selectors.of(filter);
        Deadline deadline = Deadlines.current(cluster.options());
        Partial<List<Long>> counts = Fanout.each(
                cluster.nodes(scope),
                cluster.options(),
                node -> List.of(retryOnceIfGone(node, subscription, () -> {
                    String mbean = cluster.queueMBean(node, subscription.name(), deadline);
                    JolokiaRequest request = selector.isEmpty()
                            ? JolokiaRequest.read(mbean, MESSAGE_COUNT)
                            : JolokiaRequest.exec(mbean, "countMessages(java.lang.String)", selector);
                    JolokiaResponse res = cluster.jolokia(node).single(request, deadline, Optional.of(subscription));
                    requireOk(res, node, "countMessages");
                    JsonNode v = selector.isEmpty() ? res.attribute(MESSAGE_COUNT) : res.value();
                    return v == null ? 0L : v.asLong();
                })));
        return counts.map(values -> new Total(
                selector.isEmpty() ? Denominator.ALL : Denominator.MATCHING,
                Count.exact(values.stream().mapToLong(Long::longValue).sum())));
    }

    @Override
    public Partial<Map<String, Long>> countBy(ResourceId subscription, Scope scope, MessageField field) {
        requireQueue(subscription);
        String property =
                switch (field.source()) {
                    case PROPERTY -> field.name();
                    case HEADER -> headerProperty(field.name());
                };
        Deadline deadline = Deadlines.current(cluster.options());
        Partial<List<Map<String, Long>>> groups = Fanout.each(
                cluster.nodes(scope),
                cluster.options(),
                node -> List.of(retryOnceIfGone(node, subscription, () -> {
                    String mbean = cluster.queueMBean(node, subscription.name(), deadline);
                    JolokiaResponse res = cluster.jolokia(node)
                            .single(
                                    JolokiaRequest.exec(
                                            mbean, "countMessages(java.lang.String,java.lang.String)", "", property),
                                    deadline,
                                    Optional.of(subscription));
                    requireOk(res, node, "countMessages");
                    Map<String, Long> out = new LinkedHashMap<>();
                    JsonNode grouped = res.valueParsed(cluster.mapper());
                    if (grouped != null && grouped.isObject()) {
                        grouped.properties()
                                .forEach(e -> out.put(e.getKey(), e.getValue().asLong()));
                    }
                    return out;
                })));
        return groups.map(all -> {
            Map<String, Long> merged = new LinkedHashMap<>();
            all.forEach(m -> m.forEach((k, v) -> merged.merge(k, v, Long::sum)));
            return merged;
        });
    }

    private static String headerProperty(String header) {
        return switch (header) {
            case "correlationId" -> BrowseRows.CORRELATION_ID;
            case "type" -> BrowseRows.TYPE;
            case "replyTo" -> BrowseRows.REPLY_TO;
            case "groupId" -> BrowseRows.GROUP_ID;
            case "deduplicationId" -> BrowseRows.DUPLICATE_ID;
            case "origin" -> BrowseRows.ORIGIN;
            default -> throw Queries.invalid("Artemis cannot count by the header " + header + ".");
        };
    }

    // ---- send ---------------------------------------------------------------

    @Override
    public SendReceipt send(SendTarget target, Scope scope, OutboundMessage message) {
        Deadline deadline = Deadlines.current(cluster.options());
        BrokerException last = null;
        for (NodeHandle node : cluster.nodes(scope)) {
            try {
                return node.core().isPresent()
                        ? sendOverCore(node, target, message, deadline)
                        : sendOverJolokia(node, target, message, deadline);
            } catch (BrokerException e) {
                if (!ErrorReasons.RESOURCE_GONE.equals(e.reason())) {
                    throw e;
                }
                last = e;
            }
        }
        throw last != null ? last : Queries.invalid("The scope names no node.");
    }

    /** Over Core: the message as it was given, typed properties and all. */
    private SendReceipt sendOverCore(NodeHandle node, SendTarget target, OutboundMessage message, Deadline deadline) {
        ResourceId resource;
        String destination;
        switch (target) {
            case SendTarget.ToSubscription s -> {
                requireQueue(s.subscription());
                resource = s.subscription();
                destination = Destinations.of(cluster, node, resource, deadline);
            }
            case SendTarget.ToDestination d -> {
                requireAddress(d);
                resource = d.destination();
                destination = resource.name();
            }
        }
        String id = core.call(node, "send", Optional.of(resource), deadline, session -> {
            try (CoreBodies.Built built =
                            CoreBodies.write(session, message, cluster.options().clock(), Optional.empty());
                    ClientProducer producer = session.createProducer()) {
                producer.send(SimpleString.of(destination), built.message());
                Object userId = built.message().getUserID();
                return userId == null ? null : "ID:" + userId;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        return new SendReceipt(Optional.ofNullable(id), Optional.empty(), Optional.of(node.id()));
    }

    /**
     * Over management, for a node without a Core endpoint: {@code sendMessage} takes string
     * properties only, so a typed property arrives as its text.
     */
    private SendReceipt sendOverJolokia(
            NodeHandle node, SendTarget target, OutboundMessage message, Deadline deadline) {
        Map<String, Object> properties = sendProperties(message);
        byte[] body = message.body()
                .bytes()
                .map(b -> b.toByteArray())
                .orElseThrow(() -> Queries.invalid(
                        "A message sent over Jolokia carries its whole body in memory; stream large ones over Core."));
        String text = utf8(body);
        boolean asText = text != null
                && message.headers()
                        .contentType()
                        .map(t -> t.startsWith("text/") || t.contains("json") || t.contains("xml"))
                        .orElse(true);
        // The broker makes the sender the message's validated user, so the management account sends it.
        return sendOn(
                node,
                target,
                properties,
                asText ? TEXT : BYTES,
                asText ? text : Base64.getEncoder().encodeToString(body),
                message.headers().durable().orElse(true),
                cluster.credentials().forPlane(Plane.MANAGEMENT),
                deadline);
    }

    private SendReceipt sendOn(
            NodeHandle node,
            SendTarget target,
            Map<String, Object> properties,
            int type,
            String payload,
            boolean durable,
            Credentials account,
            Deadline deadline) {
        JolokiaClient client = cluster.jolokia(node);
        ResourceId resource;
        String mbean;
        switch (target) {
            case SendTarget.ToSubscription s -> {
                requireQueue(s.subscription());
                resource = s.subscription();
                mbean = cluster.queueMBean(node, resource.name(), deadline);
            }
            case SendTarget.ToDestination d -> {
                requireAddress(d);
                resource = d.destination();
                mbean = ArtemisMBeans.address(client.brokerName(deadline), resource.name());
            }
        }
        JolokiaResponse res = client.single(
                JolokiaRequest.exec(
                        mbean,
                        SEND,
                        properties,
                        type,
                        payload,
                        durable,
                        account.username(),
                        account.password().reveal(),
                        true),
                deadline,
                Optional.of(resource));
        if (!res.ok()) {
            if (res.errorType() != null && res.errorType().contains(Errors.MBEAN_ABSENT)) {
                cluster.forgetQueue(node, resource.name());
                throw gone(resource, node);
            }
            throw Errors.refusal(node.id(), res, "sendMessage");
        }
        JsonNode id = res.value();
        return new SendReceipt(
                id == null || id.isNull() || id.asString().isBlank() ? Optional.empty() : Optional.of(id.asString()),
                Optional.empty(),
                Optional.of(node.id()));
    }

    /**
     * The headers and properties as {@code sendMessage} takes them: strings, under the broker's own
     * names. The operation takes a {@code Map<String,String>}, so a typed property arrives as its text.
     * A header Jolokia cannot carry is refused rather than dropped.
     */
    static Map<String, Object> sendProperties(OutboundMessage message) {
        MessageHeaders h = message.headers();
        if (h.priority().isPresent() || h.expiresAt().isPresent() || h.key().isPresent() || h.id().isPresent()) {
            throw Queries.invalid(
                    "A message sent over Jolokia cannot set its id, priority, expiry or key; send it over Core.");
        }
        Map<String, Object> out = new HashMap<>();
        message.properties().forEach((name, value) -> out.put(name, value.asText()));
        // Headers last: a header wins over a property of the same broker name.
        h.correlationId().ifPresent(v -> out.put(BrowseRows.CORRELATION_ID, v));
        h.type().ifPresent(v -> out.put(BrowseRows.TYPE, v));
        h.replyTo().ifPresent(v -> out.put(BrowseRows.REPLY_TO, v));
        h.groupId().ifPresent(v -> out.put(BrowseRows.GROUP_ID, v));
        h.deduplicationId().ifPresent(v -> out.put(BrowseRows.DUPLICATE_ID, v));
        h.deliverAt().ifPresent(v -> out.put(BrowseRows.SCHEDULED_DELIVERY, String.valueOf(v.toEpochMilli())));
        return out;
    }

    private static @Nullable String utf8(byte[] body) {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(body))
                    .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    // ---- bulk ---------------------------------------------------------------

    @Override
    public Partial<BulkOutcome> bulk(ResourceId subscription, Scope scope, Selection selection, MessageAction action) {
        requireQueue(subscription);
        if (action instanceof MessageAction.Move(ResourceId to)) {
            requireQueue(to);
        }
        if (action instanceof MessageAction.Copy(ResourceId to)) {
            requireQueue(to);
        }
        Deadline deadline = Deadlines.current(cluster.options());
        Partial<List<BulkOutcome>> outcomes = Fanout.each(
                cluster.nodes(scope),
                cluster.options(),
                node -> List.of(retryOnceIfGone(node, subscription, () -> {
                    String mbean = cluster.queueMBean(node, subscription.name(), deadline);
                    return switch (selection) {
                        case Selection.ById byId -> byIds(node, subscription, mbean, ids(byId, node), action, deadline);
                        case Selection.ByFilter f ->
                            byFilter(
                                    node,
                                    subscription,
                                    mbean,
                                    Selectors.bounded(Selectors.of(f.filter()), f.producedAtOrBefore()),
                                    false,
                                    action,
                                    deadline);
                        case Selection.All all ->
                            byFilter(
                                    node,
                                    subscription,
                                    mbean,
                                    Selectors.bounded("", all.producedAtOrBefore()),
                                    all.producedAtOrBefore().isEmpty(),
                                    action,
                                    deadline);
                    };
                })));
        return outcomes.map(ArtemisMessages::combine);
    }

    /** The ids of the refs that are on this node. */
    private static List<Long> ids(Selection.ById byId, NodeHandle node) {
        List<Long> ids = new ArrayList<>();
        for (MessageRef ref : byId.refs()) {
            if (ref.shard() instanceof Shard.NodeInstance(NodeId at) && at.equals(node.id())) {
                try {
                    ids.add(Long.parseLong(ref.position()));
                } catch (NumberFormatException e) {
                    throw Queries.invalid("Not an Artemis message id: " + ref.position());
                }
            }
        }
        return ids;
    }

    private BulkOutcome byIds(
            NodeHandle node,
            ResourceId subscription,
            String mbean,
            List<Long> ids,
            MessageAction action,
            Deadline deadline) {
        LongFunction<JolokiaRequest> request =
                switch (action) {
                    case MessageAction.Move(ResourceId to) ->
                        id -> JolokiaRequest.exec(mbean, "moveMessage(long,java.lang.String)", id, to.name());
                    case MessageAction.Copy(ResourceId to) ->
                        id -> JolokiaRequest.exec(mbean, "copyMessage(long,java.lang.String)", id, to.name());
                    case MessageAction.Delete d -> id -> JolokiaRequest.exec(mbean, "removeMessage(long)", id);
                    case MessageAction.Retry r -> id -> JolokiaRequest.exec(mbean, "retryMessage(long)", id);
                    case MessageAction.DeadLetter d ->
                        id -> JolokiaRequest.exec(mbean, "sendMessageToDeadLetterAddress(long)", id);
                };
        return eachId(node, subscription, ids, request, deadline);
    }

    private BulkOutcome byFilter(
            NodeHandle node,
            ResourceId subscription,
            String mbean,
            String selector,
            boolean everything,
            MessageAction action,
            Deadline deadline) {
        return switch (action) {
            case MessageAction.Move(ResourceId to) ->
                BulkOutcome.of(counted(
                        node,
                        subscription,
                        JolokiaRequest.exec(
                                mbean, "moveMessages(java.lang.String,java.lang.String)", selector, to.name()),
                        deadline));
            case MessageAction.Delete d ->
                BulkOutcome.of(counted(
                        node,
                        subscription,
                        JolokiaRequest.exec(mbean, "removeMessages(java.lang.String)", selector),
                        deadline));
            case MessageAction.DeadLetter d ->
                BulkOutcome.of(counted(
                        node,
                        subscription,
                        JolokiaRequest.exec(mbean, "sendMessagesToDeadLetterAddress(java.lang.String)", selector),
                        deadline));
            case MessageAction.Retry r
            when everything ->
                BulkOutcome.of(counted(node, subscription, JolokiaRequest.exec(mbean, "retryMessages()"), deadline));
            // Artemis has no by-filter retry and no bulk copy: the matching ids are listed and acted on.
            case MessageAction.Retry r ->
                byIds(
                        node,
                        subscription,
                        mbean,
                        listIds(node, subscription, mbean, selector, deadline),
                        action,
                        deadline);
            case MessageAction.Copy c ->
                byIds(
                        node,
                        subscription,
                        mbean,
                        listIds(node, subscription, mbean, selector, deadline),
                        action,
                        deadline);
        };
    }

    private long counted(NodeHandle node, ResourceId subscription, JolokiaRequest request, Deadline deadline) {
        JolokiaResponse res = cluster.jolokia(node).single(request, deadline, Optional.of(subscription));
        requireOk(res, node, request.operation());
        return res.value() == null ? 0L : res.value().asLong();
    }

    private List<Long> listIds(
            NodeHandle node, ResourceId subscription, String mbean, String selector, Deadline deadline) {
        JolokiaResponse res = cluster.jolokia(node)
                .single(
                        JolokiaRequest.exec(mbean, "listMessages(java.lang.String)", selector),
                        deadline,
                        Optional.of(subscription));
        requireOk(res, node, "listMessages");
        List<Long> ids = new ArrayList<>();
        JsonNode listed = res.valueParsed(cluster.mapper());
        if (listed != null) {
            listed.forEach(m -> ids.add(m.path("messageID").asLong()));
        }
        return ids;
    }

    /**
     * Acts on the ids in batches and counts those the broker says it acted on. A batch runs every
     * operation in it even after one fails, so all its results count before the run stops; later
     * batches are not sent. A failure that acted on nothing at all is thrown, because nothing changed.
     */
    private BulkOutcome eachId(
            NodeHandle node,
            ResourceId subscription,
            List<Long> ids,
            LongFunction<JolokiaRequest> request,
            Deadline deadline) {
        long done = 0;
        for (int from = 0; from < ids.size(); from += BY_ID_BATCH) {
            List<Long> chunk = ids.subList(from, Math.min(ids.size(), from + BY_ID_BATCH));
            List<JolokiaResponse> responses;
            try {
                responses = cluster.jolokia(node)
                        .batch(chunk.stream().map(request::apply).toList(), deadline, Optional.of(subscription));
            } catch (BrokerException e) {
                if (done == 0) {
                    throw e;
                }
                return new BulkOutcome(done, refs(subscription, node, ids.subList(from, ids.size())), e.getMessage());
            }
            List<Long> refused = new ArrayList<>();
            BrokerException first = null;
            for (int i = 0; i < chunk.size(); i++) {
                JolokiaResponse res = i < responses.size() ? responses.get(i) : null;
                if (res != null && res.ok()) {
                    if (res.value() != null && res.value().asBoolean()) {
                        done++;
                    }
                    continue;
                }
                refused.add(chunk.get(i));
                if (first == null) {
                    first = res == null
                            ? Errors.badResponse(node.id(), "The broker returned fewer results than operations.", null)
                            : Errors.refusal(node.id(), res, "message action");
                }
            }
            if (first != null) {
                if (done == 0) {
                    throw first;
                }
                List<Long> notDone = new ArrayList<>(refused);
                notDone.addAll(ids.subList(from + chunk.size(), ids.size()));
                return new BulkOutcome(done, refs(subscription, node, notDone), first.getMessage());
            }
        }
        return BulkOutcome.of(done);
    }

    private static List<MessageRef> refs(ResourceId subscription, NodeHandle node, List<Long> ids) {
        return ids.stream()
                .map(id -> new MessageRef(subscription, Shard.node(node.id()), String.valueOf(id)))
                .toList();
    }

    private static BulkOutcome combine(List<BulkOutcome> outcomes) {
        long done = 0;
        List<MessageRef> notDone = new ArrayList<>();
        String reason = "";
        for (BulkOutcome o : outcomes) {
            done += o.done();
            notDone.addAll(o.notDone());
            if (reason.isEmpty()) {
                reason = o.notDoneReason();
            }
        }
        return new BulkOutcome(done, notDone, reason);
    }

    // ---- helpers ------------------------------------------------------------

    /** Runs a call on a node; a queue the broker reports gone is looked up once more, as it may be recreated. */
    private <T> T retryOnceIfGone(NodeHandle node, ResourceId queue, Supplier<T> call) {
        try {
            return call.get();
        } catch (BrokerException e) {
            if (!ErrorReasons.RESOURCE_GONE.equals(e.reason())) {
                throw e;
            }
            cluster.forgetQueue(node, queue.name());
            return call.get();
        }
    }

    private NodeHandle oneNode(Scope scope, String message) {
        List<NodeHandle> nodes = cluster.nodes(scope);
        if (nodes.size() != 1) {
            throw Queries.invalid(message);
        }
        return nodes.getFirst();
    }

    private static void requireAddress(SendTarget.ToDestination d) {
        if (!d.destination().kind().equals(ArtemisKinds.ADDRESS)) {
            throw Queries.invalid("An Artemis destination is an " + ArtemisKinds.ADDRESS + ".");
        }
        if (d.partition().isPresent() || d.routingKey().isPresent()) {
            throw Queries.invalid("An Artemis address takes no partition and no routing key.");
        }
    }

    private static void requireQueue(ResourceId id) {
        if (!id.kind().equals(ArtemisKinds.QUEUE)) {
            throw Queries.invalid(
                    "Artemis holds messages in a queue, " + ArtemisKinds.QUEUE + "; not " + id.kind() + ".");
        }
    }

    private static void requireOk(JolokiaResponse res, NodeHandle node, String operation) {
        if (!res.ok()) {
            throw Errors.refusal(node.id(), res, operation);
        }
    }

    private static BrokerException gone(ResourceId resource, NodeHandle node) {
        return new BrokerException(
                Status.NOT_FOUND,
                ErrorInfo.of(ErrorReasons.RESOURCE_GONE),
                Dispatch.NOT_SENT,
                node.id(),
                resource + " is not on this node.");
    }
}
