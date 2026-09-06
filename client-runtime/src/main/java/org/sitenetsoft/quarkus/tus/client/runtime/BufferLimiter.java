package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Caps a {@code Multi<Buffer>} at exactly {@code maxBytes} emitted bytes.
 *
 * <p>{@code Multi.select().first(n)} only counts <em>items</em>, so it can't cut a stream mid-buffer.
 * This truncates the final buffer to land exactly on {@code maxBytes} regardless of how the upstream
 * chunked its output, then cancels the upstream subscription instead of draining it — that's what lets
 * {@link org.sitenetsoft.quarkus.tus.client.runtime.source.FileUploadSource} close its file handle
 * promptly instead of reading the rest of the file only to discard it.
 *
 * <p><strong>Back-pressure and cancellation flow through.</strong> Downstream demand is forwarded to
 * the upstream one-for-one (never {@code Long.MAX_VALUE}), so a file source is paced by how fast the
 * PATCH body is actually being written rather than read at disk speed into the emitter's queue. And
 * whenever the limited stream terminates — completes, fails, or is cancelled by a failed or cancelled
 * PATCH — the upstream subscription is cancelled, so the source stops reading and releases its
 * resource instead of running on to {@code maxBytes} for nobody.
 *
 * <p>Subscribes with the raw {@code Flow.Subscription} (rather than the convenience {@code with(item,
 * failure, complete)} overload that only hands back a {@code Cancellable} after subscribing returns)
 * so the subscription is available for cancellation from inside the very first {@code onNext} — even
 * for an eager upstream (e.g. {@code Multi.createFrom().items(...)}) that would otherwise drain every
 * item, and fire {@code onComplete}, before a post-hoc cancel had anything left to cut off.
 *
 * <p>If the upstream completes on its own before {@code maxBytes} bytes were delivered, that's a
 * short read against the chunk the caller asked for (the caller is expected to pass exactly
 * {@code min(chunkSize, remaining)}, so anything short of that is a real problem, not the ordinary
 * final chunk of an upload) and fails with a {@link TusClientException} naming both lengths.
 */
final class BufferLimiter {

    private BufferLimiter() {
    }

    static Multi<Buffer> limit(Multi<Buffer> upstream, long maxBytes) {
        if (maxBytes <= 0) {
            return Multi.createFrom().empty();
        }
        return Multi.createFrom().emitter(emitter -> {
            AtomicLong remaining = new AtomicLong(maxBytes);
            AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();
            // Demand that arrived before the upstream handed over its subscription (a downstream that
            // requests inside its own onSubscribe does that) is parked here and flushed once it can be.
            AtomicLong pendingDemand = new AtomicLong(0);
            Runnable flushDemand = () -> {
                Flow.Subscription s = subscription.get();
                if (s == null) {
                    return;
                }
                long demand = pendingDemand.getAndSet(0);
                if (demand > 0) {
                    s.request(demand);
                }
            };
            emitter.onRequest(n -> {
                pendingDemand.accumulateAndGet(n, BufferLimiter::saturatingAdd);
                flushDemand.run();
            });
            // Mutiny hands the emitter to the downstream's onSubscribe BEFORE running this consumer,
            // and most subscribers (collect(), the Vert.x body pipe) request from inside onSubscribe
            // -- so that demand predates the onRequest callback above and would otherwise be lost.
            // It is still visible as emitter.requested(); pick it up here. (A request racing this
            // exact line can be counted twice; over-requesting the upstream by a chunk's worth is
            // harmless -- the limit below still cuts at maxBytes -- whereas under-requesting hangs.)
            pendingDemand.accumulateAndGet(emitter.requested(), BufferLimiter::saturatingAdd);
            // Completion, failure, or downstream cancellation: the upstream never needs to produce
            // another byte. Cancelling an already-terminated subscription is a no-op per Reactive
            // Streams, so this is safe on every path.
            emitter.onTermination(() -> {
                Flow.Subscription s = subscription.get();
                if (s != null) {
                    s.cancel();
                }
            });
            upstream.subscribe().with(
                    s -> {
                        // Per Reactive Streams, onSubscribe always precedes onNext, so this runs
                        // (and the subscription is stored) before any buffer can arrive -- no race
                        // with an eager/synchronous upstream that drains on the first request().
                        subscription.set(s);
                        flushDemand.run();
                    },
                    buffer -> {
                        long rem = remaining.get();
                        if (rem <= 0) {
                            return;
                        }
                        if (buffer.length() <= rem) {
                            remaining.addAndGet(-buffer.length());
                            emitter.emit(buffer);
                        } else {
                            emitter.emit(buffer.getBuffer(0, (int) rem));
                            remaining.set(0);
                        }
                        if (remaining.get() <= 0) {
                            emitter.complete();
                        }
                    },
                    emitter::fail,
                    () -> {
                        long rem = remaining.get();
                        if (rem > 0) {
                            long actual = maxBytes - rem;
                            emitter.fail(new TusClientException("Upload source produced fewer bytes than this chunk "
                                    + "needed: expected " + maxBytes + " bytes, got " + actual));
                        } else {
                            emitter.complete();
                        }
                    });
        });
    }

    private static long saturatingAdd(long a, long b) {
        long sum = a + b;
        return sum < 0 ? Long.MAX_VALUE : sum;
    }
}
