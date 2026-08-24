package org.sitenetsoft.quarkus.tus.client.runtime.error;

public class TusClientException extends RuntimeException {
    public TusClientException(String message) {
        super(message);
    }

    public TusClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
