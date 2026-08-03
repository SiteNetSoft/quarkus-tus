package org.sitenetsoft.quarkus.tus.runtime.auth;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.sitenetsoft.quarkus.tus.runtime.TusUploadResource;

@Provider
public class TusAuthFilter implements ContainerRequestFilter {

    @ConfigProperty(name = "quarkus.tus.auth-enabled", defaultValue = "false")
    boolean authEnabled;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (!authEnabled) {
            return;
        }

        // Matched against where the endpoints are actually mounted, never against
        // configuration: a mismatch would silently let every TUS request through.
        String requestPath = requestContext.getUriInfo().getPath();
        if (!isTusPath(requestPath)) {
            return;
        }

        // Allow OPTIONS through unconditionally (TUS discovery)
        if ("OPTIONS".equalsIgnoreCase(requestContext.getMethod())) {
            return;
        }

        if (requestContext.getSecurityContext().getUserPrincipal() == null) {
            requestContext.abortWith(
                    Response.status(Response.Status.UNAUTHORIZED)
                            .entity("Authentication required")
                            .build()
            );
        }
    }

    private static boolean isTusPath(String requestPath) {
        String normalized = requestPath.startsWith("/") ? requestPath : "/" + requestPath;
        return normalized.equals(TusUploadResource.TUS_PATH)
                || normalized.startsWith(TusUploadResource.TUS_PATH + "/");
    }
}
