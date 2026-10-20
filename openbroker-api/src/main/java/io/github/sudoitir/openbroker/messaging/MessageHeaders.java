package io.github.sudoitir.openbroker.messaging;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import org.jspecify.annotations.Nullable;

/**
 * The standard headers every system's messages map onto.
 *
 * @param key the record key that decides the partition and compaction (Kafka)
 * @param groupId the ordering group: messages with the same group are delivered in order to one
 *     consumer (JMSXGroupID, SQS MessageGroupId, Service Bus SessionId, Pub/Sub orderingKey)
 * @param deduplicationId the id the system deduplicates sends by, where it does
 */
public record MessageHeaders(
        Optional<String> id,
        Optional<String> correlationId,
        Optional<String> replyTo,
        Optional<String> contentType,
        Optional<String> type,
        OptionalInt priority,
        Optional<Instant> expiresAt,
        Optional<Boolean> durable,
        Optional<Instant> producedAt,
        Optional<Instant> deliverAt,
        OptionalInt deliveryCount,
        Optional<Blob> key,
        Optional<String> groupId,
        Optional<String> deduplicationId,
        Optional<String> origin) {

    private static final MessageHeaders EMPTY = builder().build();

    public MessageHeaders {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(correlationId, "correlationId");
        Objects.requireNonNull(replyTo, "replyTo");
        Objects.requireNonNull(contentType, "contentType");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(priority, "priority");
        Objects.requireNonNull(expiresAt, "expiresAt");
        Objects.requireNonNull(durable, "durable");
        Objects.requireNonNull(producedAt, "producedAt");
        Objects.requireNonNull(deliverAt, "deliverAt");
        Objects.requireNonNull(deliveryCount, "deliveryCount");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(deduplicationId, "deduplicationId");
        Objects.requireNonNull(origin, "origin");
    }

    public static MessageHeaders empty() {
        return EMPTY;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        Builder b = new Builder();
        b.id = id.orElse(null);
        b.correlationId = correlationId.orElse(null);
        b.replyTo = replyTo.orElse(null);
        b.contentType = contentType.orElse(null);
        b.type = type.orElse(null);
        b.priority = priority.isPresent() ? priority.getAsInt() : null;
        b.expiresAt = expiresAt.orElse(null);
        b.durable = durable.orElse(null);
        b.producedAt = producedAt.orElse(null);
        b.deliverAt = deliverAt.orElse(null);
        b.deliveryCount = deliveryCount.isPresent() ? deliveryCount.getAsInt() : null;
        b.key = key.orElse(null);
        b.groupId = groupId.orElse(null);
        b.deduplicationId = deduplicationId.orElse(null);
        b.origin = origin.orElse(null);
        return b;
    }

    public static final class Builder {
        private @Nullable String id;
        private @Nullable String correlationId;
        private @Nullable String replyTo;
        private @Nullable String contentType;
        private @Nullable String type;
        private @Nullable Integer priority;
        private @Nullable Instant expiresAt;
        private @Nullable Boolean durable;
        private @Nullable Instant producedAt;
        private @Nullable Instant deliverAt;
        private @Nullable Integer deliveryCount;
        private @Nullable Blob key;
        private @Nullable String groupId;
        private @Nullable String deduplicationId;
        private @Nullable String origin;

        private Builder() {}

        public Builder id(@Nullable String id) {
            this.id = id;
            return this;
        }

        public Builder correlationId(@Nullable String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder replyTo(@Nullable String replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        public Builder contentType(@Nullable String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder type(@Nullable String type) {
            this.type = type;
            return this;
        }

        public Builder priority(@Nullable Integer priority) {
            this.priority = priority;
            return this;
        }

        public Builder expiresAt(@Nullable Instant expiresAt) {
            this.expiresAt = expiresAt;
            return this;
        }

        public Builder durable(@Nullable Boolean durable) {
            this.durable = durable;
            return this;
        }

        public Builder producedAt(@Nullable Instant producedAt) {
            this.producedAt = producedAt;
            return this;
        }

        public Builder deliverAt(@Nullable Instant deliverAt) {
            this.deliverAt = deliverAt;
            return this;
        }

        public Builder deliveryCount(@Nullable Integer deliveryCount) {
            this.deliveryCount = deliveryCount;
            return this;
        }

        public Builder key(@Nullable Blob key) {
            this.key = key;
            return this;
        }

        public Builder groupId(@Nullable String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder deduplicationId(@Nullable String deduplicationId) {
            this.deduplicationId = deduplicationId;
            return this;
        }

        /** Where a message the system moved aside (dead-lettered, expired) was first sent; a send ignores it. */
        public Builder origin(@Nullable String origin) {
            this.origin = origin;
            return this;
        }

        public MessageHeaders build() {
            return new MessageHeaders(
                    Optional.ofNullable(id),
                    Optional.ofNullable(correlationId),
                    Optional.ofNullable(replyTo),
                    Optional.ofNullable(contentType),
                    Optional.ofNullable(type),
                    priority == null ? OptionalInt.empty() : OptionalInt.of(priority),
                    Optional.ofNullable(expiresAt),
                    Optional.ofNullable(durable),
                    Optional.ofNullable(producedAt),
                    Optional.ofNullable(deliverAt),
                    deliveryCount == null ? OptionalInt.empty() : OptionalInt.of(deliveryCount),
                    Optional.ofNullable(key),
                    Optional.ofNullable(groupId),
                    Optional.ofNullable(deduplicationId),
                    Optional.ofNullable(origin));
        }
    }
}
