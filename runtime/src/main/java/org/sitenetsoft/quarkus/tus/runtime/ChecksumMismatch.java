package org.sitenetsoft.quarkus.tus.runtime;

/**
 * Signals that a chunk's {@code Upload-Checksum} did not match the bytes that arrived. Thrown
 * inside the write pipeline and mapped to {@code 460} by the resource; never leaves the
 * extension.
 */
final class ChecksumMismatch extends RuntimeException {

    ChecksumMismatch() {
        super("Checksum mismatch", null, false, false);
    }
}
