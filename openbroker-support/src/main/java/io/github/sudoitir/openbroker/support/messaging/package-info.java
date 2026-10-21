/**
 * The messaging engine a driver composes: a bounded {@link
 * io.github.sudoitir.openbroker.support.messaging.SendPipeline send pipeline}, a {@link
 * io.github.sudoitir.openbroker.support.messaging.ProcessorEngine processor engine}, a {@link
 * io.github.sudoitir.openbroker.support.messaging.LinkSupervisor link supervisor} and {@link
 * io.github.sudoitir.openbroker.support.messaging.Backoffs backoffs}. They know no broker: the driver
 * supplies the wire through small interfaces and gets the same bounds, ordering, recovery and retry
 * behaviour on every system. All of them run on virtual threads, take locks with {@code
 * ReentrantLock}, and never run user code on a caller's I/O thread.
 */
@NullMarked
package io.github.sudoitir.openbroker.support.messaging;

import org.jspecify.annotations.NullMarked;
