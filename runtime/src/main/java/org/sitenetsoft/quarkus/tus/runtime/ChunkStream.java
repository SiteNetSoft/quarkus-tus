package org.sitenetsoft.quarkus.tus.runtime;

import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import io.vertx.mutiny.core.http.HttpServerRequest;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
    private final MessageDigest digest;
    private final AtomicLong count = new AtomicLong();
    private final AtomicBoolean subscribed = new AtomicBoolean();

    /**
     * @param digest        the digest to feed, or null when no checksum was requested
     * @param maxChunkSize  fail past this many bytes with {@code Kind.CHUNK_SIZE}
     * @param maxRemaining  fail past this many bytes with {@code Kind.ENTITY_LENGTH}
     */
    ChunkStream(RoutingContext routingContext, MessageDigest digest, long maxChunkSize, long maxRemaining) {
        this.digest = digest;
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
                        throw new ChunkLimitExceededException(
                                ChunkLimitExceededException.Kind.CHUNK_SIZE, maxChunkSize);
                    }
                    if (total > maxRemaining) {
                        throw new ChunkLimitExceededException(
                                ChunkLimitExceededException.Kind.ENTITY_LENGTH, maxRemaining);
                    }
                    if (digest != null) {
                        digest.update(buffer.getByteBuf().nioBuffer());
                    }
                });
    }

    Multi<Buffer> multi() {
        return multi;
    }

    /** Whether the body has been (or is being) read; if not, it is still paused in Vert.x. */
    boolean subscribed() {
        return subscribed.get();
    }

    /** Bytes that have flowed through so far. */
    long count() {
        return count.get();
    }

    /** True if no digest was requested, or the computed digest equals the client's Base64 value. */
    boolean checksumMatches(ChecksumInfo expected) {
        if (digest == null || expected == null) {
            return true;
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
