package org.sitenetsoft.quarkus.tus.runtime.event;

public record TusConcatenationCompletedEvent(
        String finalUploadId,
        String[] partialUploadIds,
        long totalSize,
        String metadata,
        String uploaderId
) {}
