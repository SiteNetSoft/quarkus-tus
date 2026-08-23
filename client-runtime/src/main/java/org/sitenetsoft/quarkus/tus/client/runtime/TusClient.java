package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.buffer.Buffer;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusChecksumMismatchException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusOffsetMismatchException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusServerErrorException;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUpload;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadProgress;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadResult;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * High-level TUS client: capability discovery, creation, and the chunked upload loop, on top of the
 * low-level {@link TusProtocolClient}.
 *
 * <p><strong>Scope (Task 7):</strong> the sequential happy path — one chunk after another, in order,
 * over a single connection. Checksum digesting, defer-length and parallel upload are later tasks;
 * requesting them here fails fast with {@link UnsupportedOperationException} rather than silently
 * ignoring the option.
 *
 * <p><strong>Retry/resume (Task 8):</strong> a chunk PATCH that fails with a retryable error — a 5xx
 * ({@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusServerErrorException}), a 409 offset
 * conflict ({@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusOffsetMismatchException}), a
 * 460 checksum mismatch
 * ({@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusChecksumMismatchException}), or any
 * failure that isn't a {@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException} at
 * all (treated as an I/O-level failure, e.g. a reset connection) — is retried, provided the source is
 * {@link UploadSource#replayable()}: the loop waits {@code min(retryBackoff * 2^attempt,
 * retryBackoffMax)} (via {@code Uni.onItem().delayIt()}, never a blocking sleep), re-resolves the true
 * offset with a HEAD ({@link TusProtocolClient#offset(String)}), and resumes the chunk loop from
 * there. Every other {@code TusClientException} (4xx client errors like 413, protocol errors, etc.)
 * fails fast with no retry. A retryable failure against a non-replayable source fails immediately with
 * a message naming that. Attempts are consecutive-failure counters: a successful chunk resets the
 * counter to zero, so {@code maxRetries} bounds a run of consecutive failures, not the whole upload.
 * The initial {@code create()} call itself is not retried by this loop — a failure there propagates
 * as-is.
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
        return uploadRange(upload, source, from, to, chunkSize, onProgress, 0);
    }

    /**
     * @param attempt the number of consecutive chunk failures seen so far in this run (0 on the very
     *                first try, and reset to 0 again after any chunk that succeeds — see the class
     *                Javadoc's Task 8 note).
     */
    private Uni<Long> uploadRange(TusUpload upload, UploadSource source, long from, long to, long chunkSize,
            Consumer<TusUploadProgress> onProgress, int attempt) {
        if (from >= to) {
            return Uni.createFrom().item(from);
        }
        long end = Math.min(from + chunkSize, to);
        long len = end - from;
        Multi<Buffer> chunk = BufferLimiter.limit(source.slice(from), len);
        TusPatchOptions patchOptions = TusPatchOptions.builder().contentLength(len).build();

        return protocol.patch(upload.url(), from, chunk, patchOptions)
                .invoke(newOffset -> reportProgress(onProgress, newOffset, to))
                // A successful chunk resets the attempt counter: only *consecutive* failures count
                // against maxRetries, so a long upload that hits one transient error every so often
                // keeps making progress instead of eventually exhausting its retry budget.
                .flatMap(newOffset -> uploadRange(upload, source, newOffset, to, chunkSize, onProgress, 0))
                .onFailure(this::isRetryable)
                .recoverWithUni(failure -> retryChunk(upload, source, to, chunkSize, onProgress, attempt, failure));
    }

    /**
     * Handles one retryable chunk failure: gives up (propagating {@code failure}) if the source can't
     * be replayed or the retry budget is exhausted, otherwise backs off, resyncs the true offset with
     * a HEAD, and resumes the chunk loop from there.
     */
    private Uni<Long> retryChunk(TusUpload upload, UploadSource source, long to, long chunkSize,
            Consumer<TusUploadProgress> onProgress, int attempt, Throwable failure) {
        if (!source.replayable()) {
            return Uni.createFrom().failure(new TusClientException(
                    "Upload failed and the source is not replayable, so it cannot be resumed: "
                            + failure.getMessage(),
                    failure));
        }
        int nextAttempt = attempt + 1;
        if (nextAttempt > options.maxRetries()) {
            return Uni.createFrom().failure(failure);
        }
        LOG.debugf(failure, "TUS chunk upload failed (attempt %d/%d), backing off and resyncing offset",
                nextAttempt, options.maxRetries());
        return Uni.createFrom().voidItem()
                .onItem().delayIt().by(backoffFor(attempt))
                .flatMap(ignored -> protocol.offset(upload.url()))
                .flatMap(resyncedOffset -> uploadRange(upload, source, resyncedOffset, to, chunkSize, onProgress,
                        nextAttempt));
    }

    /**
     * {@code min(retryBackoff * 2^attempt, retryBackoffMax)}. {@code attempt} is the number of prior
     * consecutive failures (0 for the delay before the first retry), so the wait doubles on each
     * further consecutive failure.
     */
    private Duration backoffFor(int attempt) {
        long multiplier = 1L << Math.min(attempt, 30);
        Duration backoff = options.retryBackoff().multipliedBy(multiplier);
        Duration max = options.retryBackoffMax();
        return backoff.compareTo(max) > 0 ? max : backoff;
    }

    /**
     * Retryable: a 5xx, a 409 offset conflict, a 460 checksum mismatch, or anything that isn't even a
     * {@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException} (treated as an
     * I/O-level failure). Every other {@code TusClientException} — 4xx client errors, protocol
     * errors, etc. — fails fast.
     */
    private boolean isRetryable(Throwable failure) {
        return failure instanceof TusServerErrorException
                || failure instanceof TusOffsetMismatchException
                || failure instanceof TusChecksumMismatchException
                || !(failure instanceof TusClientException);
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
