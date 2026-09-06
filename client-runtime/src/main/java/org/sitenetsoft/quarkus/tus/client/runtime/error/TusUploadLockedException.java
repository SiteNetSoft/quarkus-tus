package org.sitenetsoft.quarkus.tus.client.runtime.error;

/**
 * The server answered {@code 423 Locked}: another request currently holds this upload's lock.
 * Typically transient -- most often the server is still holding the lock from a PATCH whose
 * connection dropped -- so the high-level {@link org.sitenetsoft.quarkus.tus.client.runtime.TusClient}
 * treats it as retryable (backoff, HEAD resync, resume) rather than as a fatal client error.
 */
public class TusUploadLockedException extends TusClientException {
    public TusUploadLockedException(String message) {
        super(message);
    }
}
