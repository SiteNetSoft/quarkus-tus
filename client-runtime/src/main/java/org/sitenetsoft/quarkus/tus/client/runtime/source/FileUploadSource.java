package org.sitenetsoft.quarkus.tus.client.runtime.source;

import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.file.AsyncFile;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException;

import java.io.IOException;
import java.nio.file.Path;

/**
 * An {@link UploadSource} backed by a file on disk.
 *
 * <p>Each {@link #slice(long)} call opens the file afresh via the Vert.x filesystem, on whatever
 * context the caller is subscribing from — {@code AsyncFile} is context-bound, so opening it once and
 * reusing it across subscribers is unsafe. Opening fresh per slice keeps every call independent and
 * makes the source fully re-readable.
 */
class FileUploadSource implements UploadSource {

    private final io.vertx.core.Vertx vertx;
    private final Path path;
    private final long length;

    FileUploadSource(io.vertx.core.Vertx vertx, Path path) {
        this.vertx = vertx;
        this.path = path;
        try {
            this.length = java.nio.file.Files.size(path);
        } catch (IOException e) {
            throw new TusClientException("Unable to determine the size of " + path, e);
        }
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public Multi<Buffer> slice(long fromOffset) {
        Vertx mutinyVertx = Vertx.newInstance(vertx);
        OpenOptions openOptions = new OpenOptions().setRead(true).setWrite(false).setCreate(false);
        return mutinyVertx.fileSystem().open(path.toString(), openOptions)
                .onItem().transform(asyncFile -> asyncFile.setReadPos(fromOffset))
                .onItem().transformToMulti(this::readAndClose);
    }

    private Multi<Buffer> readAndClose(AsyncFile asyncFile) {
        return asyncFile.toMulti()
                .map(io.vertx.mutiny.core.buffer.Buffer::getDelegate)
                .onTermination().call((failure, cancelled) -> asyncFile.close());
    }
}
