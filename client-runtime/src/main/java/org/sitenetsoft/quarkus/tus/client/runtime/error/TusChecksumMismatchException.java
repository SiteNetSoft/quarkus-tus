package org.sitenetsoft.quarkus.tus.client.runtime.error;

public class TusChecksumMismatchException extends TusClientException {
    public TusChecksumMismatchException(String message) {
        super(message);
    }
}
