package org.sitenetsoft.quarkus.tus.client.runtime.error;

public class TusProtocolException extends TusClientException {
    public TusProtocolException(String message) {
        super(message);
    }
}
