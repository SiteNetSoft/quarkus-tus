package org.sitenetsoft.quarkus.tus.runtime.sse;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseEventSink;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.TusUploadAuthorizer;
import org.sitenetsoft.quarkus.tus.runtime.TusUtils;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

@Path("/tus/events")
public class TusSseResource {

    private static final Logger LOG = Logger.getLogger(TusSseResource.class);

    @ConfigProperty(name = "quarkus.tus.sse-enabled", defaultValue = "true")
    boolean sseEnabled;

    @Inject
    TusSseService sseService;

    @Inject
    UploadStore uploadStore;

    @Inject
    TusUploadAuthorizer authorizer;

    @Inject
    Sse sse;

    // Sink-based SSE methods run on a worker, so the store is awaited here — bounded, and a
    // registration is rare next to the chunk traffic it observes.
    private static final java.time.Duration STORE_TIMEOUT = java.time.Duration.ofSeconds(10);

    @GET
    @io.smallrye.common.annotation.Blocking
    @Path("/{uploadId}")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void streamUploadEvents(
            @PathParam("uploadId") String uploadId,
            @Context SecurityContext securityContext,
            @Context SseEventSink eventSink
    ) {
        if (!sseEnabled) {
            throw new NotFoundException();
        }
        if (!TusUtils.isValidUuid(uploadId)) {
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

        // Registering a sink displaces any existing subscriber, so an unknown or unowned ID
        // must not get this far. 404 rather than 403 keeps a denial indistinguishable from a
        // missing upload.
        UploadInfo info = uploadStore.findUploadInfo(uploadId).await().atMost(STORE_TIMEOUT).orElse(null);
        if (info == null || authorizer.isDenied(info, securityContext)) {
            throw new NotFoundException();
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
