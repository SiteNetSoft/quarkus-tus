package org.sitenetsoft.quarkus.tus.client.runtime.source;

import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An {@link UploadSource} wrapping a single, already-in-flight {@code Multi}. The data can only be
 * consumed once, from offset zero: {@link #slice(long)} rejects a non-zero offset outright, and
 * rejects a second call regardless of offset once the stream has been handed out.
 */
class OneShotUploadSource implements UploadSource {

    private final Multi<Buffer> data;
    private final long declaredLength;
    private final AtomicBoolean consumed = new AtomicBoolean(false);

    OneShotUploadSource(Multi<Buffer> data, long declaredLength) {
        this.data = data;
        this.declaredLength = declaredLength;
    }

    @Override
    public long length() {
        return declaredLength;
    }

    @Override
    public Multi<Buffer> slice(long fromOffset) {
        if (fromOffset != 0) {
            throw new IllegalStateException("A one-shot upload source cannot be sliced from offset " + fromOffset
                    + "; it can only be read once, from the beginning.");
        }
        if (!consumed.compareAndSet(false, true)) {
            throw new IllegalStateException("A one-shot upload source can only be read once.");
        }
        return data;
    }

    @Override
    public boolean replayable() {
        return false;
    }
}
