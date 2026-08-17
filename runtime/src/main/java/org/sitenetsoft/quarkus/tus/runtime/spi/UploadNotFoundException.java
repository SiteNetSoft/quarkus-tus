package org.sitenetsoft.quarkus.tus.runtime.spi;

/**
 * Raised by a store when asked to act on an upload id it does not hold. The extension maps
 * this to {@code 404 Not Found}.
 */
public class UploadNotFoundException extends RuntimeException {

    private final String uploadId;

    public UploadNotFoundException(String uploadId) {
        super("No such upload: " + uploadId);
        this.uploadId = uploadId;
    }

    public String getUploadId() {
        return uploadId;
    }
}
