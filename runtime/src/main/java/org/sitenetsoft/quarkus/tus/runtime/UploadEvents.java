package org.sitenetsoft.quarkus.tus.runtime;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.sitenetsoft.quarkus.tus.runtime.event.TusChunkReceivedEvent;
import org.sitenetsoft.quarkus.tus.runtime.event.TusConcatenationCompletedEvent;
import org.sitenetsoft.quarkus.tus.runtime.event.TusUploadCompletedEvent;
import org.sitenetsoft.quarkus.tus.runtime.event.TusUploadCreatedEvent;
import org.sitenetsoft.quarkus.tus.runtime.event.TusUploadTerminatedEvent;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadProgress;
import org.sitenetsoft.quarkus.tus.runtime.sse.TusSseService;

import java.util.List;

/**
 * Everything the extension tells the outside world about an upload: the CDI lifecycle events,
 * the progress bookkeeping behind them, and the SSE stream. Nothing else fires an
 * {@code Event<...>}, so what an application observes is decided in one place rather than in
 * whichever code path happened to reach the milestone.
 * <p>
 * Observers are application code, so callers arrange to be off the event loop before calling in.
 */
@ApplicationScoped
public class UploadEvents {

    @Inject
    UploadProgressService uploadProgressService;

    @Inject
    Instance<TusSseService> sseServiceInstance;

    @Inject
    Event<TusUploadCreatedEvent> uploadCreatedEvent;

    @Inject
    Event<TusChunkReceivedEvent> chunkReceivedEvent;

    @Inject
    Event<TusUploadTerminatedEvent> uploadTerminatedEvent;

    @Inject
    Event<TusUploadCompletedEvent> uploadCompletedEvent;

    @Inject
    Event<TusConcatenationCompletedEvent> concatenationCompletedEvent;

    /** An upload record now exists. A deferred length has nothing to track progress against yet. */
    public void uploadCreated(String uploadId, long size, boolean deferredLength, boolean partial, String metadata) {
        if (!deferredLength) {
            uploadProgressService.startUpload(uploadId, size);
        }
        uploadCreatedEvent.fire(new TusUploadCreatedEvent(uploadId, size, deferredLength, partial, metadata));
    }

    /** A deferred length became known, so progress has something to measure against. */
    public void lengthKnown(String uploadId, long length) {
        uploadProgressService.startUpload(uploadId, length);
    }

    /**
     * A chunk landed — including the empty one some clients send to poll, which advances nothing
     * but should still say where the upload stands.
     */
    public void chunkReceived(String uploadId, long chunkSize, long newOffset, long entityLength) {
        if (chunkSize > 0) {
            uploadProgressService.updateProgress(uploadId, chunkSize);
        }
        chunkReceivedEvent.fire(new TusChunkReceivedEvent(uploadId, chunkSize, newOffset, entityLength));
        sendProgress(uploadId, newOffset, entityLength);
    }

    /**
     * The last byte is in. The progress stream has already been told 100% by the chunk event
     * and has nothing more to say, so it is closed here; the events stream is the bridge's,
     * which closes it on the completion event unless a consumer holds it open.
     */
    public void uploadCompleted(String uploadId, UploadInfo info) {
        uploadProgressService.finishUpload(uploadId);
        try {
            uploadCompletedEvent.fire(new TusUploadCompletedEvent(
                    uploadId, info.getEntityLength(), info.getMetadata(), info.getUploaderId()));
        } finally {
            if (sseServiceInstance.isResolvable()) {
                sseServiceInstance.get().unregister(uploadId);
            }
        }
    }

    public void uploadTerminated(String uploadId) {
        if (sseServiceInstance.isResolvable()) {
            sseServiceInstance.get().unregister(uploadId);
        }
        uploadTerminatedEvent.fire(new TusUploadTerminatedEvent(uploadId));
    }

    public void concatenationCompleted(String finalId, List<String> partialIds, UploadInfo info) {
        concatenationCompletedEvent.fire(new TusConcatenationCompletedEvent(
                finalId, partialIds.toArray(new String[0]), info.getEntityLength(),
                info.getMetadata(), info.getUploaderId()));
    }

    /**
     * An upload is gone — deleted, expired, merged away or abandoned. Nothing will ever be
     * sent about it again, so both of its streams close with it; a stream held open past a
     * completion that already happened is left to its consumer's {@code finish} or the backstop.
     */
    public void uploadDiscarded(String uploadId) {
        uploadProgressService.finishUpload(uploadId);
        if (sseServiceInstance.isResolvable()) {
            TusSseService sse = sseServiceInstance.get();
            sse.unregister(uploadId);
            sse.onUploadDiscarded(uploadId);
        }
    }

    /**
     * Tells the progress stream where the upload stands. Progress entries live in memory, so
     * after a restart the store knows the upload but the progress service does not — the event
     * is then built from the offset itself, or a watcher would stall on the previous chunk and
     * never see 100%.
     */
    private void sendProgress(String uploadId, long offset, long entityLength) {
        if (!sseServiceInstance.isResolvable()) {
            return;
        }
        UploadProgress progress = uploadProgressService.getProgress(uploadId);
        if (progress == null && entityLength >= 0) {
            progress = new UploadProgress(entityLength);
            progress.uploadedBytes = offset;
        }
        if (progress != null) {
            sseServiceInstance.get().sendProgress(uploadId, progress);
        }
    }
}
