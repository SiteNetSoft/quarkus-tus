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
        ENTITY_LENGTH,
        /**
         * More bytes than remain before {@code quarkus.tus.max-size}, while the upload's length
         * is still deferred (no {@code Upload-Length} to check {@code ENTITY_LENGTH} against
         * yet). Unlike {@code ENTITY_LENGTH} -- which means the client is violating a length it
         * already agreed to, a conflict -- this is the server's own cap and maps to 413, same as
         * {@code CHUNK_SIZE}.
         */
        MAX_SIZE
    }

    private final Kind kind;

    ChunkLimitExceededException(Kind kind, long limit) {
        super(switch (kind) {
            case CHUNK_SIZE -> "Chunk exceeds maximum allowed size of " + limit + " bytes";
            case ENTITY_LENGTH -> "Chunk exceeds declared upload size by more than " + limit + " remaining bytes";
            case MAX_SIZE -> "Chunk exceeds maximum allowed upload size by more than " + limit + " remaining bytes";
        });
        this.kind = kind;
    }

    Kind kind() {
        return kind;
    }
}
