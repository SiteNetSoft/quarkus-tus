package org.sitenetsoft.quarkus.tus.client.runtime.error;

public class TusUploadNotFoundException extends TusClientException {
    private final boolean expired;

    public TusUploadNotFoundException(String message, boolean expired) {
        super(message);
        this.expired = expired;
    }

    public boolean knownExpired() {
        return expired;
    }
}
