package org.sitenetsoft.quarkus.tus.client.runtime.source;

import io.smallrye.mutiny.Multi;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;

import java.nio.file.Path;

/**
 * A source of upload bytes that the client can read in independently addressable slices.
 *
 * <p>Implementations backed by durable storage (such as a file) are {@link #replayable()}: calling
 * {@link #slice(long)} more than once, or with different offsets, always yields correct, independent
 * data. That re-readability is what gates resume, checksum verification, and parallel concatenation
 * uploads. A source built from a single in-flight {@code Multi} (see {@link #oneShot(Multi, long)}) is
 * not replayable: it can be sliced from offset zero exactly once.
 */
public interface UploadSource {

    /**
     * The total number of bytes this source will produce, or {@code -1} if the length is not known
     * up front (defer-length).
     */
    long length();

    /**
     * Returns an independent, re-readable stream of the bytes starting at {@code fromOffset}.
     *
     * @param fromOffset the byte offset to start the slice at
     * @return a fresh {@code Multi} producing the bytes from {@code fromOffset} to the end of the source
     */
    Multi<Buffer> slice(long fromOffset);

    /**
     * Whether this source supports being sliced more than once (and from arbitrary offsets). Resume,
     * checksum verification, and parallel concatenation all require a replayable source.
     */
    default boolean replayable() {
        return true;
    }

    /**
     * An {@code UploadSource} backed by a file on disk. Each call to {@link #slice(long)} opens the
     * file afresh, so the source is fully replayable.
     */
    static UploadSource ofFile(Vertx vertx, Path path) {
        return new FileUploadSource(vertx, path);
    }

    /**
     * An {@code UploadSource} that wraps a single, already-in-flight {@code Multi}. This is a degraded
     * mode: the data can only be consumed once, from offset zero, so resume, checksum, and parallel
     * upload are unavailable for it.
     *
     * @param data           the single-use stream of bytes to upload
     * @param declaredLength the total length of {@code data}, or {@code -1} if unknown
     */
    static UploadSource oneShot(Multi<Buffer> data, long declaredLength) {
        return new OneShotUploadSource(data, declaredLength);
    }
}
