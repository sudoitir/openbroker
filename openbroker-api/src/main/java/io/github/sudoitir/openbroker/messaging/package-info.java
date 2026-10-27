/**
 * Messages: the message model, browsing, counting, bulk actions, and the messaging client
 * ({@link io.github.sudoitir.openbroker.messaging.Transport}).
 *
 * <h2>Guarantees</h2>
 * Each sender and receiver declares a {@link io.github.sudoitir.openbroker.messaging.Guarantee}:
 * {@code AT_MOST_ONCE}, {@code AT_LEAST_ONCE} or {@code TRANSACTED}. {@link
 * io.github.sudoitir.openbroker.messaging.Transport#capabilities()} is static; a link outside it
 * fails {@code UNIMPLEMENTED}, request not sent. {@code AT_LEAST_ONCE} and {@code TRANSACTED} sends
 * are durable; an explicit non-durable message is refused. "Effectively once" is {@code
 * AT_LEAST_ONCE} with a {@code DeduplicationScope} above {@code NONE}, which uses the system's
 * deduplication (keyed by the {@code deduplicationId} header in {@code WINDOW}). An ask above the
 * transport's declared scope fails {@code UNIMPLEMENTED}, request not sent. Ordering with {@code
 * AT_LEAST_ONCE} requires at least {@code SESSION}.
 *
 * <h2>Bounded sending</h2>
 * A {@link io.github.sudoitir.openbroker.messaging.SendBuffer} bounds messages accepted but not yet
 * confirmed. {@code offer} never blocks and returns {@link
 * io.github.sudoitir.openbroker.messaging.Offer.Full} when there is no room; {@code send} waits up to
 * {@code maxBlock}, then fails {@code RESOURCE_EXHAUSTED}, and otherwise waits for the confirmation.
 * Non-transacted senders are thread-safe; a transacted sender has one owner.
 *
 * <h2>Flow control</h2>
 * {@link io.github.sudoitir.openbroker.messaging.FlowControl} bounds the messages handed out and
 * unsettled, and the client's read-ahead in the unit the system enforces
 * ({@link io.github.sudoitir.openbroker.messaging.PrefetchBound}).
 *
 * <h2>Processing lanes</h2>
 * {@link io.github.sudoitir.openbroker.messaging.Transport#process} runs a handler on virtual threads
 * in {@code concurrency} lanes. A lane holds at most one unsettled message, so settlement is per
 * message. {@link io.github.sudoitir.openbroker.messaging.OrderingScope} keeps a group, a partition
 * or the whole subscription in order. The retry policy is the system's, or managed by the client:
 * in place when ordered, by delayed release when not, and dead-lettered after {@code maxAttempts}.
 *
 * <h2>Recovery</h2>
 * The driver reopens a lost link on its node and then on the serving member of its HA group, under
 * the link's {@link io.github.sudoitir.openbroker.messaging.Recovery}. State changes reach {@link
 * io.github.sudoitir.openbroker.messaging.LinkListener}. After recovery:
 * <ul>
 *   <li>unconfirmed {@code AT_LEAST_ONCE} sends are resent in order, and the receipt says {@code
 *       resent}; without deduplication that can duplicate;
 *   <li>{@code AT_MOST_ONCE} sends are never resent; their receipts fail {@code UNAVAILABLE},
 *       {@code IN_DOUBT};
 *   <li>settling a message received before recovery fails {@code FAILED_PRECONDITION}
 *       ({@code DELIVERY_STALE}), because it will be delivered again;
 *   <li>a transacted commit fails {@code ABORTED}; an unanswered commit is {@code IN_DOUBT}, and a
 *       redo with the same deduplication ids is safe;
 *   <li>an unsettled message is redelivered, so handlers see duplicates;
 *   <li>after {@code giveUpAfter} the link is {@code FAILED}, pending receipts fail {@code
 *       UNAVAILABLE} ({@code IN_DOUBT}) and nothing is resent.
 * </ul>
 *
 * <h2>Observer</h2>
 * {@link io.github.sudoitir.openbroker.messaging.MessagingObserver} watches send, receive, process
 * and settle, and may inject trace context into message properties. The default does nothing and
 * allocates nothing.
 */
@NullMarked
package io.github.sudoitir.openbroker.messaging;

import org.jspecify.annotations.NullMarked;
