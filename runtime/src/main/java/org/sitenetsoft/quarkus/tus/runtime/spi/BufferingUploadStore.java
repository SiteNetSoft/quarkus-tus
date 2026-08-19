package org.sitenetsoft.quarkus.tus.runtime.spi;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.buffer.Buffer;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An {@link UploadStore} for backends that would rather append a whole {@code byte[]} than
 * consume a stream.
 * <p>
 * The staged-write contract is satisfied by holding each staged chunk in memory:
 * {@link #stageChunk} collects the stream, {@link #commitChunk} hands the bytes to
 * {@link #appendBytes} and advances the record, {@link #abortChunk} drops them. That buffers a
 * chunk per in-flight upload — which is the point of choosing this class. A store that cares
 * about memory implements the three methods directly.
 * <p>
 * Subclasses implement the record methods ({@link #findUploadInfo}, {@link #createUpload},
 * {@link #updateUploadInfo}), {@link #appendBytes}, {@link #concatenate}, {@link #discardUpload},
 * the lock pair and {@link #cleanupExpiredUploads}. Only {@link #appendBytes} may block; the
 * record methods are subscribed to on the event loop like every other SPI method, and a store
 * whose records are in memory simply returns {@code Uni.createFrom().item(...)}.
 */
public abstract class BufferingUploadStore implements UploadStore {

    private final Map<String, byte[]> staged = new ConcurrentHashMap<>();

    /**
     * Appends {@code data} to the upload's bytes. Called from {@link #commitChunk} while the
     * framework holds the upload's lock; {@code offset} is the upload's current offset, i.e.
     * where the first byte lands. Store bytes only — the adapter advances the record.
     */
    protected abstract void appendBytes(String id, long offset, byte[] data);

    @Override
    public Uni<Long> stageChunk(String id, long offset, Multi<Buffer> data, long expectedLength) {
        return findUploadInfo(id).chain(found -> {
            UploadInfo info = found.orElse(null);
            if (info == null) {
                return Uni.createFrom().failure(new UploadNotFoundException(id));
            }
            if (offset != info.getOffset()) {
                return Uni.createFrom().failure(new OffsetMismatchException(
                        "Write at offset " + offset + " but upload " + id + " is at offset " + info.getOffset(),
                        info.getOffset()));
            }
            return data.collect().in(ByteArrayOutputStream::new, (out, buf) -> out.writeBytes(buf.getBytes()))
                    .onItem().transform(out -> {
                        byte[] bytes = out.toByteArray();
                        staged.put(id, bytes);
                        return (long) bytes.length;
                    });
        });
    }

    @Override
    public Uni<Void> commitChunk(String id, long offset, long bytesStaged) {
        byte[] bytes = staged.remove(id);
        if (bytes == null) {
            if (bytesStaged != 0) {
                return Uni.createFrom().failure(new IllegalStateException(
                        "commitChunk(" + id + ") without a staged chunk"));
            }
            bytes = new byte[0];
        }
        final byte[] data = bytes;
        return findUploadInfo(id).chain(found -> {
            UploadInfo info = found.orElse(null);
            if (info == null) {
                return Uni.createFrom().<Void>failure(new UploadNotFoundException(id));
            }
            // appendBytes is synchronous by design — this class exists for stores whose client
            // blocks — so it must not run on the event loop the body arrived on.
            return Uni.createFrom().<Void>item(() -> {
                        appendBytes(id, offset, data);
                        return null;
                    })
                    .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                    .chain(() -> {
                        info.setOffset(offset + data.length);
                        info.setLastActivity(Instant.now());
                        return updateUploadInfo(id, info);
                    });
        });
    }

    @Override
    public Uni<Void> abortChunk(String id, long offset) {
        staged.remove(id);
        return Uni.createFrom().voidItem();
    }
}
