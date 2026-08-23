package org.sitenetsoft.quarkus.tus.runtime.sse;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.sitenetsoft.quarkus.tus.runtime.event.TusChunkReceivedEvent;
import org.sitenetsoft.quarkus.tus.runtime.event.TusUploadCompletedEvent;
import org.sitenetsoft.quarkus.tus.runtime.event.TusUploadTerminatedEvent;

/**
 * Feeds the {@code /tus/events/{uploadId}} stream from the CDI lifecycle events.
 * <p>
 * The stream previously delivered only its opening {@code connected} event: the sink map was
 * written to on subscribe and read on unsubscribe, but nothing ever sent to it, so a client saw a
 * healthy connection that never updated. The events already existed and fired; they just were not
 * connected to the stream.
 * <p>
 * {@code complete} and {@code terminated} also close the stream, which is what the documentation
 * promises ("stays open until the upload completes") and is the only path that removes a sink on a
 * normal, successful upload.
 */
@ApplicationScoped
public class TusSseEventBridge {

    @Inject
    TusSseService sseService;

    void onChunk(@Observes TusChunkReceivedEvent event) {
        sseService.sendUploadEvent(event.uploadId(), "progress", String.format(
                "{\"bytesUploaded\": %d, \"totalBytes\": %d, \"chunkSize\": %d}",
                event.newOffset(), event.totalSize(), event.chunkSize()));
    }

    // The resource fires the chunk event before the completion event, so by the time this runs
    // the stream has already reached 100%; a zero-length upload completes with no chunk at all.
    // Completion normally also ends the stream, but a consumer that holds it open keeps sending
    // its own events past this point and closes it with TusSseService.finish.
    void onCompleted(@Observes TusUploadCompletedEvent event) {
        sseService.sendUploadEvent(event.uploadId(), "complete", String.format(
                "{\"bytesUploaded\": %d, \"totalBytes\": %d}",
                event.totalSize(), event.totalSize()));
        sseService.onUploadCompleted(event.uploadId());
    }

    void onTerminated(@Observes TusUploadTerminatedEvent event) {
        sseService.sendUploadEvent(event.uploadId(), "terminated", "{}");
        sseService.unregisterUpload(event.uploadId());
    }
}
