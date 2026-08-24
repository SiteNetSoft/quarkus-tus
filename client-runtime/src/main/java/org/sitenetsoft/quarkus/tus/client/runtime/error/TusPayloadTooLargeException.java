package org.sitenetsoft.quarkus.tus.client.runtime.error;

public class TusPayloadTooLargeException extends TusClientException {
    public TusPayloadTooLargeException(String message) {
        super(message);
    }
}
