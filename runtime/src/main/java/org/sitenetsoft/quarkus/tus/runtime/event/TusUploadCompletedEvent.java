package org.sitenetsoft.quarkus.tus.runtime.event;

public record TusUploadCompletedEvent(
        String uploadId,
        long totalSize,
        String metadata,
        String uploaderId
) {}
