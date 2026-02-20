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
        sinks.put(clientId, sink);
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

    public void registerForUpload(String uploadId, SseEventSink sink) {
        if (uploadId == null || sink == null) {
            return;
        }
        uploadSinks.put(uploadId, sink);
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

    public void sendStatus(String uploadId, String status, String message, String filename) {
        if (uploadId == null || status == null) return;

        var sink = uploadSinks.get(uploadId);
        if (sink == null) return;

        if (sink.isClosed()) {
            uploadSinks.remove(uploadId);
            return;
        }

        try {
            StringBuilder json = new StringBuilder();
            json.append("{\"status\": \"").append(status).append("\"");
            if (message != null) {
                String escapedMessage = message.replace("\\", "\\\\")
                        .replace("\"", "\\\"")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r")
                        .replace("\t", "\\t");
                json.append(", \"message\": \"").append(escapedMessage).append("\"");
            }
            if (filename != null) {
                String escapedFilename = filename.replace("\\", "\\\\")
                        .replace("\"", "\\\"");
                json.append(", \"filename\": \"").append(escapedFilename).append("\"");
            }
            json.append("}");

            sink.send(sse.newEventBuilder()
                    .name("status")
                    .data(json.toString())
                    .build());
        } catch (Exception e) {
            LOG.debugf("Failed to send status to upload %s: %s", uploadId, e.getMessage());
            uploadSinks.remove(uploadId);
            try { sink.close(); } catch (Exception ignored) {}
        }
    }

    public void sendCustomEvent(String uploadId, String eventName, String jsonPayload) {
        if (uploadId == null || eventName == null) return;

        var sink = uploadSinks.get(uploadId);
        if (sink == null) return;

        if (sink.isClosed()) {
            uploadSinks.remove(uploadId);
            return;
        }

        try {
            sink.send(sse.newEventBuilder()
                    .name(eventName)
                    .data(jsonPayload != null ? jsonPayload : "{}")
                    .build());
        } catch (Exception e) {
            LOG.debugf("Failed to send custom event to upload %s: %s", uploadId, e.getMessage());
            uploadSinks.remove(uploadId);
            try { sink.close(); } catch (Exception ignored) {}
        }
    }

    public boolean hasListener(String uploadId) {
        if (uploadId == null) return false;
        SseEventSink sink = uploadSinks.get(uploadId);
        return sink != null && !sink.isClosed();
    }
}
