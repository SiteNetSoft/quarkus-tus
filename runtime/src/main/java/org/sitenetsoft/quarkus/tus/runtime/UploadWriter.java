package org.sitenetsoft.quarkus.tus.runtime;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.OffsetMismatchException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadNotFoundException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;

import java.security.MessageDigest;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Writes one chunk: the request body streams into the store, and the framework then commits it
 * or throws it away. This is the whole of the staged write — the store's part of it is
 * {@code stageChunk}/{@code commitChunk}/{@code abortChunk}, and the decision between the last
 * two is made here, because only the framework knows the protocol rules that settle it.
 * <p>
 * It answers with the upload's new offset, or fails with the reason it could not get there. It
 * builds no responses and knows no status codes; the resource maps the failures.
 */
@ApplicationScoped
public class UploadWriter {

    private static final Logger LOG = Logger.getLogger(UploadWriter.class);

    @Inject
    UploadStore uploadStore;

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    @Inject
    UploadEvents events;

    /**
     * Streams the request body into the store at {@code offset}: stage, then commit if the
     * checksum matched or abort if it did not, then progress bookkeeping and events. Resolves to
     * the new offset. The caller holds the upload's lock and releases it afterwards; nothing here
     * does. Fails with {@link ChecksumMismatch}, {@link BadChecksumHeader},
     * {@link ChunkLimitExceededException}, {@link OffsetMismatchException},
     * {@link UploadNotFoundException} or the store's failure.
     * <p>
     * {@code digests} holds every algorithm being computed as the bytes pass. It has one entry for
     * an {@code Upload-Checksum} header, and one per configured algorithm when the client announced
     * the checksum as a <em>trailer</em> ({@code trailerAnnounced}) — a trailer names its algorithm
     * only after the body, by which time it is too late to start hashing.
     */
    public Uni<Long> write(String uploadID, UploadInfo info, long offset, RoutingContext routingContext,
                           ChecksumInfo checksumInfo, Map<String, MessageDigest> digests,
                           boolean trailerAnnounced, Long contentLength,
                           AtomicReference<ChunkStream> streamRef) {
        final long entityLength = info.getEntityLength();
        // While the length is deferred there is no Upload-Length to bound the stream against, but
        // the byte stream itself still has to be cut off somewhere -- quarkus.tus.max-size is that
        // bound. Without this, a deferred-length client PATCHing a chunked (no Content-Length)
        // body bypasses the header-based fast rejection in TusUploadResource entirely, since that
        // check only runs when a Content-Length is present. Kind.MAX_SIZE (413) rather than
        // Kind.ENTITY_LENGTH (409): this is the server's own cap, not the client breaking a length
        // it already agreed to.
        long remaining = entityLength >= 0 ? entityLength - offset : tusRuntimeConfig.maxSize() - offset;
        ChunkLimitExceededException.Kind remainingKind = entityLength >= 0
                ? ChunkLimitExceededException.Kind.ENTITY_LENGTH
                : ChunkLimitExceededException.Kind.MAX_SIZE;
        ChunkStream stream = new ChunkStream(routingContext, digests, tusRuntimeConfig.maxChunkSize(), remaining,
                remainingKind);
        streamRef.set(stream);
        // Completion is a transition, decided once: only the commit that reaches the declared
        // length fires it, and the record it reports is re-read after that commit.
        AtomicBoolean completed = new AtomicBoolean();
        long expectedLength = contentLength != null ? contentLength : -1;

        // deferred(): a store that throws from stageChunk instead of returning a failed Uni
        // still lands in the failure path below, where the lock is released and the error mapped.
        return Uni.createFrom().deferred(() -> uploadStore.stageChunk(uploadID, offset, stream.multi(), expectedLength))
                // The limits are ours to enforce: whatever the store made of the cut-off stream —
                // wrapped it, or even reported success — the answer is what we counted.
                .onFailure().transform(e -> stream.limitExceeded() != null ? stream.limitExceeded() : e)
                .onItem().transformToUni(staged -> {
                    if (stream.limitExceeded() != null) {
                        return Uni.createFrom().<Long>failure(stream.limitExceeded());
                    }
                    ChecksumInfo expected = checksumInfo;
                    if (expected == null && trailerAnnounced) {
                        // The body is in; the trailer, if the client really sent one, is now readable.
                        String trailer = routingContext.request().getTrailer("Upload-Checksum");
                        if (trailer != null && !trailer.isBlank()) {
                            Optional<ChecksumInfo> parsed = TusUtils.parseChecksumHeader(trailer);
                            if (parsed.isEmpty()) {
                                return uploadStore.abortChunk(uploadID, offset).onItem().transformToUni(v ->
                                        Uni.createFrom().<Long>failure(
                                                new BadChecksumHeader("Malformed Upload-Checksum trailer")));
                            }
                            if (!isSupportedChecksumAlgorithm(parsed.get().algorithm())) {
                                return uploadStore.abortChunk(uploadID, offset).onItem().transformToUni(v ->
                                        Uni.createFrom().<Long>failure(
                                                new BadChecksumHeader("Unsupported checksum algorithm")));
                            }
                            expected = parsed.get();
                        }
                    }
                    if (!stream.checksumMatches(expected)) {
                        return uploadStore.abortChunk(uploadID, offset)
                                .onItem().transformToUni(v -> Uni.createFrom().<Long>failure(new ChecksumMismatch()));
                    }
                    if (staged == 0) {
                        // A length-less body that turned out empty: nothing to commit.
                        return uploadStore.abortChunk(uploadID, offset).replaceWith(offset);
                    }
                    return uploadStore.commitChunk(uploadID, offset, staged).replaceWith(offset + staged);
                })
                .onFailure(e -> !(e instanceof ChecksumMismatch) && !(e instanceof OffsetMismatchException)).call(e -> {
                    // A store that fails mid-stage may already have discarded its bytes; abort
                    // anyway so the offset is guaranteed to be where the client left it. An
                    // abort failure must not mask the original error. Not after a stale offset,
                    // though: nothing was staged, and abortChunk(offset) would tell the store to
                    // roll the upload back below where it really is.
                    return uploadStore.abortChunk(uploadID, offset)
                            .onFailure().recoverWithItem(abortErr -> {
                                LOG.warnf(abortErr, "Failed to abort staged chunk for upload %s", uploadID);
                                return null;
                            });
                })
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .onItem().invoke(newOffset -> {
                    events.chunkReceived(uploadID, newOffset - offset, newOffset, entityLength);

                    // A later empty PATCH at the final offset does not complete, nor a restart.
                    if (offset < entityLength && newOffset == entityLength) {
                        completed.set(true);
                    }
                })
                .call(newOffset -> completed.get()
                        ? uploadStore.findUploadInfo(uploadID)
                                .invoke(current -> events.uploadCompleted(uploadID, current.orElse(info)))
                                .replaceWithVoid()
                        : Uni.createFrom().voidItem());
    }

    private boolean isSupportedChecksumAlgorithm(String algorithm) {
        if (algorithm == null || algorithm.isBlank()) {
            return false;
        }
        for (String supported : tusRuntimeConfig.checksumAlgorithms().split(",")) {
            if (supported.trim().equalsIgnoreCase(algorithm.trim())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The empty chunk: nothing reaches the store — it never sees a chunk it could not write —
     * but the client still gets told where the upload stands.
     */
    public Uni<Long> writeNothing(String uploadID, long offset, long entityLength) {
        return Uni.createFrom().item(offset)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .onItem().invoke(at -> events.chunkReceived(uploadID, 0, at, entityLength));
    }
}
