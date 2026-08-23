package org.sitenetsoft.quarkus.tus.client.runtime.error;

public class TusOffsetMismatchException extends TusClientException {
    public TusOffsetMismatchException(String message) {
        super(message);
    }
}
