package org.sitenetsoft.quarkus.tus.runtime.spi;

/**
 * Raised by a store when its backend fails — the disk is full, the object store is down. The
 * extension maps this to {@code 500 Internal Server Error} and leaves the upload retryable.
 */
public class UploadStoreException extends RuntimeException {

    public UploadStoreException(String message) {
        super(message);
    }

    public UploadStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
