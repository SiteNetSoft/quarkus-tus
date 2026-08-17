package org.sitenetsoft.quarkus.tus.runtime;

/**
 * Raised inside the request body stream when the bytes actually received exceed a limit that
 * could not be enforced from {@code Content-Length} alone (chunked bodies carry none). The
 * framework aborts the staged chunk and answers 413 or 409 depending on {@link #kind()}.
 */
final class ChunkLimitExceededException extends RuntimeException {

    enum Kind {
        /** More bytes than {@code quarkus.tus.max-chunk-size}. */
        CHUNK_SIZE,
        /** More bytes than remain before {@code Upload-Length}. */
        ENTITY_LENGTH
    }

    private final Kind kind;

    ChunkLimitExceededException(Kind kind, long limit) {
        super(kind == Kind.CHUNK_SIZE
                ? "Chunk exceeds maximum allowed size of " + limit + " bytes"
                : "Chunk exceeds declared upload size by more than " + limit + " remaining bytes");
        this.kind = kind;
    }

    Kind kind() {
        return kind;
    }
}
