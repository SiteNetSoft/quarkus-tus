package org.sitenetsoft.quarkus.tus.runtime.ratelimit;

import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.sitenetsoft.quarkus.tus.runtime.TusUploadResource;

@Provider
@Priority(Priorities.USER + 1)
public class TusRateLimitFilter implements ContainerRequestFilter {

    @ConfigProperty(name = "quarkus.tus.rate-limit-enabled", defaultValue = "false")
    boolean rateLimitEnabled;

    @Inject
    TusRateLimitService rateLimitService;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (!rateLimitEnabled) {
            return;
        }

        // Matched against where the endpoints are actually mounted, never against
        // configuration: a mismatch would silently disable throttling.
        String requestPath = requestContext.getUriInfo().getPath();
        if (!isTusPath(requestPath)) {
            return;
        }

        String method = requestContext.getMethod();
        if (!"POST".equalsIgnoreCase(method) && !"PATCH".equalsIgnoreCase(method)) {
            return;
        }

        String clientId = resolveClientId(requestContext);
        TusRateLimitService.RateLimitResult result = rateLimitService.tryConsume(clientId);

        if (!result.allowed()) {
            long retryAfterSeconds = Math.max(1, (result.retryAfterMs() + 999) / 1000);
            requestContext.abortWith(
                    Response.status(429)
                            .header("Retry-After", String.valueOf(retryAfterSeconds))
                            .entity("Rate limit exceeded")
                            .build()
            );
        }
    }

    private static boolean isTusPath(String requestPath) {
        String normalized = requestPath.startsWith("/") ? requestPath : "/" + requestPath;
        return normalized.equals(TusUploadResource.TUS_PATH)
                || normalized.startsWith(TusUploadResource.TUS_PATH + "/");
    }

    private String resolveClientId(ContainerRequestContext requestContext) {
        if (requestContext.getSecurityContext() != null
                && requestContext.getSecurityContext().getUserPrincipal() != null) {
            return requestContext.getSecurityContext().getUserPrincipal().getName();
        }

        // Fall back to remote IP from headers or direct connection
        String forwarded = requestContext.getHeaderString("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return forwarded.split(",")[0].trim();
        }

        return "anonymous";
    }
}
