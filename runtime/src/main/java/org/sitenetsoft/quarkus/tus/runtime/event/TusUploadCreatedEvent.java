package org.sitenetsoft.quarkus.tus.runtime.event;

public record TusUploadCreatedEvent(
        String uploadId,
        long totalSize,
        boolean deferredLength,
        boolean partial,
        String metadata
) {}
