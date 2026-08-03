package org.sitenetsoft.quarkus.tus.runtime;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import org.sitenetsoft.quarkus.tus.runtime.config.TusBuildTimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

/**
 * Decides whether a request may act on a given upload.
 * <p>
 * Shared by every endpoint that takes an upload ID — the TUS resource and both SSE streams —
 * so the rule cannot drift between them. Callers answer {@code 404} rather than {@code 403} on
 * denial, keeping the response indistinguishable from an upload that does not exist so it
 * cannot be used to probe for other users' upload IDs.
 */
@ApplicationScoped
public class TusUploadAuthorizer {

    @Inject
    UploadStore uploadStore;

    @Inject
    TusBuildTimeConfig tusBuildTimeConfig;

    /**
     * Whether the caller may not act on this upload. Uploads with no recorded uploader —
     * created while authentication was disabled — stay accessible, so enabling auth does not
     * strand them.
     */
    public boolean isDenied(String uploadId, SecurityContext securityContext) {
        return isDenied(uploadId, currentUserId(securityContext));
    }

    /** As {@link #isDenied(String, SecurityContext)}, for callers that already resolved the user. */
    public boolean isDenied(String uploadId, String currentUserId) {
        if (!tusBuildTimeConfig.authEnabled()) {
            return false;
        }
        String ownerId = uploadStore.getUploaderId(uploadId);
        return ownerId != null && !ownerId.equals(currentUserId);
    }

    public String currentUserId(SecurityContext securityContext) {
        if (securityContext != null && securityContext.getUserPrincipal() != null) {
            return securityContext.getUserPrincipal().getName();
        }
        return null;
    }
}
