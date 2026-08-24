package org.sitenetsoft.quarkus.tus.client.runtime.error;

public class TusServerErrorException extends TusClientException {
    private final int status;

    public TusServerErrorException(String message, int status) {
        super(message);
        this.status = status;
    }

    public int status() {
        return status;
    }
}
