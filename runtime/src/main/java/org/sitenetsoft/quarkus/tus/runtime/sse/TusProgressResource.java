package org.sitenetsoft.quarkus.tus.runtime.sse;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.sse.SseEventSink;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.UploadProgressService;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadProgress;

@Path("/tus/progress")
public class TusProgressResource {

    private static final Logger LOG = Logger.getLogger(TusProgressResource.class);

    @ConfigProperty(name = "quarkus.tus.sse-enabled", defaultValue = "true")
    boolean sseEnabled;

    @Inject
    TusSseService sseService;

    @Inject
    UploadProgressService uploadProgressService;

    @GET
    @Path("/{uploadID}")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void streamProgress(
            @PathParam("uploadID") String uploadID,
            @Context SseEventSink eventSink
    ) {
        if (!sseEnabled) {
            throw new NotFoundException();
        }
        LOG.infof("Registering SSE sink for uploadID=%s", uploadID);
        sseService.register(uploadID, eventSink);

        UploadProgress current = uploadProgressService.getProgress(uploadID);
        if (current != null) {
            sseService.sendProgress(uploadID, current);
        }
    }
}
