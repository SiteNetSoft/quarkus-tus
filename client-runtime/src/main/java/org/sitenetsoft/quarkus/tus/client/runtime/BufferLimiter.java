package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.core.buffer.Buffer;

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
            AtomicReference<Cancellable> subscription = new AtomicReference<>();
            Cancellable cancellable = upstream.subscribe().with(
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
                            Cancellable s = subscription.get();
                            if (s != null) {
                                s.cancel();
                            }
                        }
                    },
                    emitter::fail,
                    emitter::complete);
            subscription.set(cancellable);
        });
    }
}
