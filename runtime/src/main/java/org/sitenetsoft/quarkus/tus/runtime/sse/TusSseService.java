package org.sitenetsoft.quarkus.tus.runtime.sse;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseEventSink;
import io.vertx.core.Vertx;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadProgress;

@ApplicationScoped
public class TusSseService {

    private static final Logger LOG = Logger.getLogger(TusSseService.class);

    private final Map<String, SseEventSink> sinks = new ConcurrentHashMap<>();
    private final Map<String, SseEventSink> uploadSinks = new ConcurrentHashMap<>();

    /**
     * Uploads whose events stream should outlive completion. The upload finishing is not the
     * story finishing: a consumer that keeps working after the last byte (moving the file
     * onward, scanning it) calls {@link #holdOpen} so its own {@link #sendUploadEvent} calls
     * still reach the client, and {@link #finish} when its pipeline is done. Completion then
     * starts the backstop timer in {@code backstopTimers} instead of closing the stream, so an
     * abandoned pipeline leaks the sink only until the timeout, never forever.
     */
    private final Set<String> heldOpen = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> backstopTimers = new ConcurrentHashMap<>();

    @Inject
    Sse sse;

    @Inject
    Vertx vertx;

    @Inject
    TusRuntimeConfig config;

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

    /**
     * Asks for the upload's events stream to stay open past completion. Call any time before
     * the upload completes; without it, completion closes the stream as always.
     */
    public void holdOpen(String uploadId) {
        if (uploadId != null) {
            heldOpen.add(uploadId);
        }
    }

    /**
     * Ends a held-open stream: the consumer's pipeline is done, nothing more will be sent.
     * Idempotent, and harmless on an upload that was never held open.
     */
    public void finish(String uploadId) {
        unregisterUpload(uploadId);
    }

    /**
     * Completion's effect on the stream: close it, unless {@link #holdOpen} asked otherwise —
     * then only start the backstop timer. Called by the bridge, not by consumers.
     */
    public void onUploadCompleted(String uploadId) {
        if (uploadId == null) return;
        if (!heldOpen.contains(uploadId)) {
            unregisterUpload(uploadId);
            return;
        }
        long timer = vertx.setTimer(config.sseHoldOpenTimeoutSeconds() * 1000, id -> {
            LOG.debugf("Hold-open backstop closing SSE stream for upload %s", uploadId);
            finish(uploadId);
        });
        Long displaced = backstopTimers.put(uploadId, timer);
        if (displaced != null) {
            vertx.cancelTimer(displaced);
        }
    }

    /**
     * The upload is gone. A stream held open past a completion that already happened has a
     * consumer still working and a backstop already armed, so it is theirs to finish; any
     * other stream for this upload has nobody left to write to it and is closed now — a hold
     * placed on an upload that never completed included, since no backstop ever covers it.
     */
    public void onUploadDiscarded(String uploadId) {
        if (uploadId == null) return;
        if (heldOpen.contains(uploadId) && backstopTimers.containsKey(uploadId)) {
            return;
        }
        unregisterUpload(uploadId);
    }

    public void unregisterUpload(String uploadId) {
        if (uploadId == null) return;
        heldOpen.remove(uploadId);
        Long timer = backstopTimers.remove(uploadId);
        if (timer != null) {
            vertx.cancelTimer(timer);
        }
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
