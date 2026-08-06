package org.sitenetsoft.quarkus.tus.runtime.spi;

/**
 * Raised by a store when a chunk's checksum does not match the value the client supplied.
 * <p>
 * The extension maps this to {@code 460 Checksum Mismatch} and leaves the upload's offset
 * untouched, so the client can resend the chunk. It lives in the SPI because an alternative
 * {@link UploadStore} must be able to signal a mismatch without depending on the default
 * implementation it replaces.
 */
public class ChecksumMismatchException extends RuntimeException {

    public ChecksumMismatchException(String message) {
        super(message);
    }
}
