package org.sitenetsoft.quarkus.tus.runtime.auth;

import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Provider
public class TusAuthFilter implements ContainerRequestFilter {

    @ConfigProperty(name = "quarkus.tus.auth-enabled", defaultValue = "false")
    boolean authEnabled;

    @ConfigProperty(name = "quarkus.tus.path", defaultValue = "/tus")
    String tusPath;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (!authEnabled) {
            return;
        }

        // Only apply to TUS endpoints
        String requestPath = requestContext.getUriInfo().getPath();
        if (!requestPath.startsWith(tusPath)) {
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
}
