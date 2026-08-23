package org.sitenetsoft.quarkus.tus.client.runtime.error;

public class TusVersionMismatchException extends TusClientException {
    public TusVersionMismatchException(String message) {
        super(message);
    }
}
