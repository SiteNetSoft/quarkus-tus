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
            upstream.subscribe().with(
                    s -> {
                        // Per Reactive Streams, onSubscribe always precedes onNext, so this runs
                        // (and the subscription is stored) before any buffer can arrive -- no race
                        // with an eager/synchronous upstream that drains on the request() call below.
                        subscription.set(s);
                        s.request(Long.MAX_VALUE);
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
                            Flow.Subscription s = subscription.get();
                            if (s != null) {
                                s.cancel();
                            }
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
}
