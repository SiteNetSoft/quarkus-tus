package org.sitenetsoft.quarkus.tus.it;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import jakarta.inject.Singleton;
import jakarta.enterprise.inject.Alternative;
import org.sitenetsoft.quarkus.tus.runtime.spi.OffsetMismatchException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadNotFoundException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStoreException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link InMemoryUploadStore} with knobs for behaving the way a hastily written third-party
 * store might: throwing synchronously instead of returning a failed {@code Uni}, or reporting a
 * stale offset. Records what the framework did in return.
 */
@Singleton
@Alternative
public class FaultyUploadStore extends InMemoryUploadStore {

    /** When set, {@link #stageChunk} throws {@link UploadNotFoundException} synchronously. */
    public final AtomicBoolean throwSyncFromStage = new AtomicBoolean();
    /** When >= 0, {@link #stageChunk} fails with {@link OffsetMismatchException} claiming this offset. */
    public final AtomicInteger reportStaleOffsetAs = new AtomicInteger(-1);
    /** When set, a failure of the body stream is wrapped in {@code UploadStoreException}, as an SDK might. */
    public final AtomicBoolean wrapStreamFailures = new AtomicBoolean();

    public final AtomicInteger abortCalls = new AtomicInteger();
    public final AtomicBoolean appendRanOnEventLoop = new AtomicBoolean();
    public volatile String lastCreatedId;

    public void reset() {
        throwSyncFromStage.set(false);
        reportStaleOffsetAs.set(-1);
        wrapStreamFailures.set(false);
        abortCalls.set(0);
        appendRanOnEventLoop.set(false);
    }

    @Override
    public String createUpload(org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo info) {
        return lastCreatedId = super.createUpload(info);
    }

    @Override
    public Uni<Long> stageChunk(String id, long offset, Multi<Buffer> data, long expectedLength) {
        if (throwSyncFromStage.get()) {
            throw new UploadNotFoundException(id);
        }
        int stale = reportStaleOffsetAs.get();
        if (stale >= 0) {
            return Uni.createFrom().failure(new OffsetMismatchException("stale offset", stale));
        }
        if (wrapStreamFailures.get()) {
            data = data.onFailure().transform(e -> new UploadStoreException("sdk wrapped: " + e, e));
        }
        return super.stageChunk(id, offset, data, expectedLength);
    }

    @Override
    public Uni<Void> abortChunk(String id, long offset) {
        abortCalls.incrementAndGet();
        return super.abortChunk(id, offset);
    }

    @Override
    protected void appendBytes(String id, long offset, byte[] data) {
        if (Context.isOnEventLoopThread()) {
            appendRanOnEventLoop.set(true);
        }
        super.appendBytes(id, offset, data);
    }
}
