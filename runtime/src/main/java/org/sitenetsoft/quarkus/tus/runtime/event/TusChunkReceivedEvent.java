package org.sitenetsoft.quarkus.tus.runtime.event;

public record TusChunkReceivedEvent(
        String uploadId,
        long chunkSize,
        long newOffset,
        long totalSize
) {}
