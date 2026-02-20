package org.sitenetsoft.quarkus.tus.runtime.event;

public record TusUploadTerminatedEvent(
        String uploadId
) {}
