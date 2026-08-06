package org.sitenetsoft.quarkus.tus.runtime.sse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseEventSink;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadProgress;

@ApplicationScoped
public class TusSseService {

    private static final Logger LOG = Logger.getLogger(TusSseService.class);

    private final Map<String, SseEventSink> sinks = new ConcurrentHashMap<>();
    private final Map<String, SseEventSink> uploadSinks = new ConcurrentHashMap<>();

    @Inject
    Sse sse;

    public void register(String clientId, SseEventSink sink) {
        if (clientId == null || sink == null) {
            return;
        }
        closeDisplaced(sinks.put(clientId, sink), sink, clientId);
    }

    /**
     * Closes a sink that a new registration replaced. Overwriting the map entry alone left the
     * previous subscriber's connection open but unreachable, so it was never closed.
     */
    private void closeDisplaced(SseEventSink previous, SseEventSink replacement, String key) {
        if (previous == null || previous == replacement) {
            return;
        }
        try {
            if (!previous.isClosed()) {
                previous.close();
            }
        } catch (Exception e) {
            LOG.debugf("Error closing displaced SSE sink for %s: %s", key, e.getMessage());
        }
    }

    public void unregister(String clientId) {
        if (clientId == null) return;
        SseEventSink removed = sinks.remove(clientId);
        if (removed != null) {
            try {
                if (!removed.isClosed()) {
                    removed.close();
                }
            } catch (Exception e) {
                LOG.debugf("Error closing SSE sink for client %s: %s", clientId, e.getMessage());
            }
        }
    }

    public void sendProgress(String clientId, UploadProgress progress) {
        if (clientId == null || progress == null) return;

        var sink = sinks.get(clientId);
        if (sink == null) return;

        if (sink.isClosed()) {
            sinks.remove(clientId);
            return;
        }

        try {
            String progressJson = String.format(
                    "{\"percentage\": %d, \"uploadedBytes\": %d, \"totalBytes\": %d}",
                    progress.getPercentage(), progress.uploadedBytes, progress.totalBytes
            );
            sink.send(sse.newEventBuilder()
                    .name("upload-progress")
                    .data(progressJson)
                    .build());
        } catch (Exception e) {
            LOG.debugf("Failed to send progress to client %s: %s", clientId, e.getMessage());
            sinks.remove(clientId);
            try { sink.close(); } catch (Exception ignored) {}
        }
    }

    /**
     * Sends a lifecycle event on the {@code /tus/events} stream for an upload.
     * <p>
     * Closed or broken sinks are dropped here, which is the only thing that keeps the map from
     * growing: nothing iterates it otherwise.
     *
     * @param uploadId  the upload whose stream to write to
     * @param eventName the SSE event name, e.g. {@code progress} or {@code complete}
     * @param json      the payload
     */
    public void sendUploadEvent(String uploadId, String eventName, String json) {
        if (uploadId == null || eventName == null) return;

        var sink = uploadSinks.get(uploadId);
        if (sink == null) return;

        if (sink.isClosed()) {
            uploadSinks.remove(uploadId);
            return;
        }

        try {
            sink.send(sse.newEventBuilder().name(eventName).data(json == null ? "{}" : json).build());
        } catch (Exception e) {
            LOG.debugf("Failed to send %s to upload stream %s: %s", eventName, uploadId, e.getMessage());
            uploadSinks.remove(uploadId);
            try { sink.close(); } catch (Exception ignored) {}
        }
    }

    public void registerForUpload(String uploadId, SseEventSink sink) {
        if (uploadId == null || sink == null) {
            return;
        }
        closeDisplaced(uploadSinks.put(uploadId, sink), sink, uploadId);
    }

    public void unregisterUpload(String uploadId) {
        if (uploadId == null) return;
        SseEventSink removed = uploadSinks.remove(uploadId);
        if (removed != null) {
            try {
                if (!removed.isClosed()) {
                    removed.close();
                }
            } catch (Exception e) {
                LOG.debugf("Error closing SSE sink for upload %s: %s", uploadId, e.getMessage());
            }
        }
    }

}
