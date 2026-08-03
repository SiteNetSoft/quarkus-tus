package org.sitenetsoft.quarkus.tus.runtime.spi;

/**
 * Raised when a write is attempted at an offset that is not the upload's current one.
 * <p>
 * A store must not write at a caller-supplied offset without checking it: doing so lets a
 * request that raced past validation overwrite bytes that were already stored and
 * acknowledged. The extension maps this to {@code 409 Conflict}.
 */
public class OffsetMismatchException extends RuntimeException {

    private final long expectedOffset;

    public OffsetMismatchException(String message, long expectedOffset) {
        super(message);
        this.expectedOffset = expectedOffset;
    }

    /** The upload's actual current offset, which the client should resume from. */
    public long getExpectedOffset() {
        return expectedOffset;
    }
}
