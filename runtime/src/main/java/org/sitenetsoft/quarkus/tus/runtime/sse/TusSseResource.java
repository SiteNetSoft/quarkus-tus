package org.sitenetsoft.quarkus.tus.runtime.sse;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseEventSink;
import org.jboss.logging.Logger;

import java.util.regex.Pattern;

@Path("/tus/events")
public class TusSseResource {

    private static final Logger LOG = Logger.getLogger(TusSseResource.class);

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            Pattern.CASE_INSENSITIVE
    );

    @Inject
    TusSseService sseService;

    @Inject
    Sse sse;

    @GET
    @Path("/{uploadId}")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void streamUploadEvents(
            @PathParam("uploadId") String uploadId,
            @Context SseEventSink eventSink
    ) {
        if (uploadId == null || !UUID_PATTERN.matcher(uploadId).matches()) {
            try {
                eventSink.send(sse.newEventBuilder()
                        .name("error")
                        .data("{\"error\": \"Invalid upload ID format\"}")
                        .build());
                eventSink.close();
            } catch (Exception e) {
                LOG.debugf("Error sending error event: %s", e.getMessage());
            }
            return;
        }

        LOG.infof("SSE connection opened for upload: %s", uploadId);

        sseService.registerForUpload(uploadId, eventSink);

        try {
            eventSink.send(sse.newEventBuilder()
                    .name("connected")
                    .data("{\"uploadId\": \"" + uploadId + "\"}")
                    .build());
        } catch (Exception e) {
            LOG.debugf("Error sending initial SSE event for upload %s: %s", uploadId, e.getMessage());
            sseService.unregisterUpload(uploadId);
        }
    }
}
