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
import jakarta.ws.rs.sse.SseEventSink;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.TusUploadAuthorizer;
import org.sitenetsoft.quarkus.tus.runtime.TusUtils;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.UploadProgressService;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadProgress;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

@Path("/tus/progress")
public class TusProgressResource {

    private static final Logger LOG = Logger.getLogger(TusProgressResource.class);

    @ConfigProperty(name = "quarkus.tus.sse-enabled", defaultValue = "true")
    boolean sseEnabled;

    @Inject
    TusSseService sseService;

    @Inject
    UploadProgressService uploadProgressService;

    @Inject
    UploadStore uploadStore;

    @Inject
    TusUploadAuthorizer authorizer;

    // Sink-based SSE methods run on a worker, so the store is awaited here — bounded, and a
    // registration is rare next to the chunk traffic it observes.
    private static final java.time.Duration STORE_TIMEOUT = java.time.Duration.ofSeconds(10);

    @GET
    @io.smallrye.common.annotation.Blocking
    @Path("/{uploadID}")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void streamProgress(
            @PathParam("uploadID") String uploadID,
            @Context SecurityContext securityContext,
            @Context SseEventSink eventSink
    ) {
        if (!sseEnabled) {
            throw new NotFoundException();
        }

        // The stream reveals an upload's size and live byte counts, and registering a sink
        // displaces any existing subscriber — so an unknown or unowned ID must not get this
        // far. 404 rather than 403 keeps a denial indistinguishable from a missing upload.
        if (!TusUtils.isValidUuid(uploadID)) {
            throw new NotFoundException();
        }
        UploadInfo info = uploadStore.findUploadInfo(uploadID).await().atMost(STORE_TIMEOUT).orElse(null);
        if (info == null || authorizer.isDenied(info, securityContext)) {
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
