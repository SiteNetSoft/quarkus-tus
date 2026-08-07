package org.sitenetsoft.quarkus.tus.runtime;

/**
 * A malformed or unsupported {@code Upload-Checksum} that only became readable after the body —
 * that is, one sent as a trailer. Mapped to {@code 400} by the resource; never leaves the
 * extension.
 */
final class BadChecksumHeader extends RuntimeException {

    BadChecksumHeader(String message) {
        super(message, null, false, false);
    }
}
