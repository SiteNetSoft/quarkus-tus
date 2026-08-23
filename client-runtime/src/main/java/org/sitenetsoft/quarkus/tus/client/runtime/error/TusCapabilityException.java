package org.sitenetsoft.quarkus.tus.client.runtime.error;

public class TusCapabilityException extends TusClientException {
    private final String extension;

    public TusCapabilityException(String message, String extension) {
        super(message);
        this.extension = extension;
    }

    public String missingExtension() {
        return extension;
    }
}
