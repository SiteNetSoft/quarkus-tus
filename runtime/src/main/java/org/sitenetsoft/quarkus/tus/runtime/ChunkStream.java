package org.sitenetsoft.quarkus.tus.runtime;

import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import io.vertx.mutiny.core.http.HttpServerRequest;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The request body as a backpressured {@link Multi} of buffers, observed on the way through:
 * bytes are counted, optionally digested for {@code Upload-Checksum}, and the stream fails with
 * {@link ChunkLimitExceededException} the moment a limit is crossed, so a store never receives
 * more than it should regardless of what {@code Content-Length} claimed.
 * <p>
 * Nothing is read from the socket until the returned {@code Multi} is subscribed to — which
 * happens inside the store's {@code stageChunk} — so a request rejected before that point has
 * an untouched body. Quarkus REST leaves the request paused when a resource method declares no
 * body parameter; {@code Expect: 100-continue} is honoured here because the framework's own
 * handling of it lives in the body reader this class replaces.
 */
final class ChunkStream {

    private final Multi<Buffer> multi;
    private final Map<String, MessageDigest> digests;
    private final AtomicLong count = new AtomicLong();
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicReference<ChunkLimitExceededException> limitExceeded = new AtomicReference<>();

    /**
     * @param digests       the digests to feed, keyed by TUS algorithm name; empty when no
     *                      checksum was requested. More than one when the algorithm is only
     *                      known at the end (an announced {@code Upload-Checksum} trailer).
     * @param maxChunkSize  fail past this many bytes with {@code Kind.CHUNK_SIZE}
     * @param maxRemaining  fail past this many bytes with {@code remainingKind}
     * @param remainingKind the kind to raise for {@code maxRemaining} -- {@code ENTITY_LENGTH}
     *                      while the upload's declared length bounds it, {@code MAX_SIZE} while
     *                      the length is still deferred and the server-wide cap bounds it instead
     */
    ChunkStream(RoutingContext routingContext, Map<String, MessageDigest> digests, long maxChunkSize, long maxRemaining,
                ChunkLimitExceededException.Kind remainingKind) {
        this.digests = digests;
        io.vertx.core.http.HttpServerRequest request = routingContext.request();
        this.multi = HttpServerRequest.newInstance(request).toMulti()
                .onSubscription().invoke(() -> {
                    subscribed.set(true);
                    if ("100-continue".equalsIgnoreCase(request.getHeader("Expect"))) {
                        request.response().writeContinue();
                    }
                })
                .onItem().transform(io.vertx.mutiny.core.buffer.Buffer::getDelegate)
                .onItem().invoke(buffer -> {
                    long total = count.addAndGet(buffer.length());
                    if (total > maxChunkSize) {
                        throw limit(new ChunkLimitExceededException(
                                ChunkLimitExceededException.Kind.CHUNK_SIZE, maxChunkSize));
                    }
                    if (total > maxRemaining) {
                        throw limit(new ChunkLimitExceededException(remainingKind, maxRemaining));
                    }
                    for (MessageDigest digest : digests.values()) {
                        digest.update(buffer.getByteBuf().nioBuffer());
                    }
                });
    }

    private ChunkLimitExceededException limit(ChunkLimitExceededException e) {
        limitExceeded.compareAndSet(null, e);
        return e;
    }

    Multi<Buffer> multi() {
        return multi;
    }

    /**
     * The limit this stream was cut off at, or null. Kept here as well as thrown down the
     * stream because the response is the framework's decision from what it counted — not
     * something to depend on the store handing the failure back in its original type.
     */
    ChunkLimitExceededException limitExceeded() {
        return limitExceeded.get();
    }

    /** Whether the body has been (or is being) read; if not, it is still paused in Vert.x. */
    boolean subscribed() {
        return subscribed.get();
    }

    /** Bytes that have flowed through so far. */
    long count() {
        return count.get();
    }

    /**
     * True if nothing was expected, or the digest computed for the expected algorithm equals the
     * client's Base64 value. False if the algorithm was not being digested — the caller decides
     * up front which algorithms it will accept.
     */
    boolean checksumMatches(ChecksumInfo expected) {
        if (expected == null) {
            return true;
        }
        MessageDigest digest = digests.get(expected.algorithm().trim().toLowerCase());
        if (digest == null) {
            return false;
        }
        String computed = Base64.getEncoder().encodeToString(digest.digest());
        return computed.equals(expected.value());
    }

    /** Maps a TUS algorithm name to a JCA digest, or empty if the name is unknown to us. */
    static Optional<MessageDigest> digestFor(String tusAlgorithm) {
        String jca = switch (tusAlgorithm.trim().toLowerCase()) {
            case "sha1" -> "SHA-1";
            case "md5" -> "MD5";
            case "sha256" -> "SHA-256";
            default -> null;
        };
        if (jca == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(MessageDigest.getInstance(jca));
        } catch (NoSuchAlgorithmException e) {
            return Optional.empty();
        }
    }
}
