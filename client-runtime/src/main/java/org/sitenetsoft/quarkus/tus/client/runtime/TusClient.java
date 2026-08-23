package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.buffer.Buffer;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUpload;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadProgress;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadResult;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;

import java.util.function.Consumer;

/**
 * High-level TUS client: capability discovery, creation, and the chunked upload loop, on top of the
 * low-level {@link TusProtocolClient}.
 *
 * <p><strong>Scope (Task 7):</strong> the sequential happy path only — one chunk after another, in
 * order, over a single connection. Retry/resume, checksum digesting, defer-length and parallel upload
 * are later tasks; requesting them here fails fast with {@link UnsupportedOperationException} rather
 * than silently ignoring the option.
 */
public class TusClient {

    private static final Logger LOG = Logger.getLogger(TusClient.class);

    private final TusClientOptions options;
    private final TusProtocolClient protocol;
    private final Uni<org.sitenetsoft.quarkus.tus.client.runtime.model.TusServerCapabilities> capabilities;

    private TusClient(TusClientOptions options, TusProtocolClient protocol) {
        this.options = options;
        this.protocol = protocol;
        // Cached per client instance: the first upload's options() call primes it, every later
        // upload on this client reuses the result instead of re-fetching capabilities.
        this.capabilities = protocol.options().memoize().indefinitely();
    }

    public static TusClient create(io.vertx.core.Vertx vertx, TusClientOptions options) {
        TusTarget.Builder targetBuilder = TusTarget.builder(options.url());
        options.connectTimeout().ifPresent(targetBuilder::connectTimeout);
        options.requestTimeout().ifPresent(targetBuilder::requestTimeout);
        options.customizer().ifPresent(targetBuilder::customizer);
        TusProtocolClient protocol = new TusProtocolClient(vertx, targetBuilder.build());
        return new TusClient(options, protocol);
    }

    public TusProtocolClient protocol() {
        return protocol;
    }

    public Uni<TusUploadResult> upload(TusUploadRequest request) {
        int parallelism = request.parallelism().orElse(options.parallelism());
        if (parallelism > 1) {
            throw new UnsupportedOperationException(
                    "Parallel uploads (parallelism > 1) are not implemented yet; that's Task 10/11.");
        }
        String checksumAlgorithm = request.checksumAlgorithm().orElse(options.checksumAlgorithm().orElse(null));
        if (checksumAlgorithm != null) {
            throw new UnsupportedOperationException(
                    "Checksum digesting is not implemented yet; that's Task 9. Leave checksumAlgorithm unset.");
        }

        UploadSource source = request.source();
        long length = source.length();
        if (length < 0) {
            throw new UnsupportedOperationException(
                    "Defer-length uploads are not implemented yet; that's Task 8. The source length must be known.");
        }

        long chunkSize = request.chunkSize().orElse(options.chunkSize());
        Consumer<TusUploadProgress> onProgress = request.onProgress().orElse(null);

        TusCreateOptions createOptions = TusCreateOptions.builder()
                .length(length)
                .metadata(request.metadata())
                .build();

        return capabilities
                .flatMap(caps -> protocol.create(createOptions))
                .flatMap(created -> uploadRange(created, source, created.offset(), length, chunkSize, onProgress)
                        .map(finalOffset -> new TusUploadResult(created.url(), finalOffset)));
    }

    /**
     * Uploads {@code [from, to)} of {@code source} to {@code upload}'s URL, one {@code chunkSize}
     * slice at a time, as a recursive {@code Uni} chain. Kept range-capable (rather than always
     * starting at 0 and always running to the source's full length) so Task 11's partial-upload
     * concatenation can reuse it for an arbitrary sub-range.
     */
    private Uni<Long> uploadRange(TusUpload upload, UploadSource source, long from, long to, long chunkSize,
            Consumer<TusUploadProgress> onProgress) {
        if (from >= to) {
            return Uni.createFrom().item(from);
        }
        long end = Math.min(from + chunkSize, to);
        long len = end - from;
        Multi<Buffer> chunk = BufferLimiter.limit(source.slice(from), len);
        TusPatchOptions patchOptions = TusPatchOptions.builder().contentLength(len).build();

        return protocol.patch(upload.url(), from, chunk, patchOptions)
                .invoke(newOffset -> reportProgress(onProgress, newOffset, to))
                .flatMap(newOffset -> uploadRange(upload, source, newOffset, to, chunkSize, onProgress));
    }

    private void reportProgress(Consumer<TusUploadProgress> onProgress, long bytesSent, long total) {
        if (onProgress == null) {
            return;
        }
        try {
            onProgress.accept(new TusUploadProgress(bytesSent, total));
        } catch (RuntimeException e) {
            // A misbehaving progress callback must not break the upload itself, but it shouldn't
            // vanish silently either.
            LOG.debugf(e, "TusUploadRequest onProgress callback threw for bytesSent=%d, totalBytes=%d",
                    bytesSent, total);
        }
    }

    /**
     * Closes the underlying HTTP client. Same event-loop caveat as {@link TusProtocolClient#close()}.
     */
    public void close() {
        protocol.close();
    }
}
