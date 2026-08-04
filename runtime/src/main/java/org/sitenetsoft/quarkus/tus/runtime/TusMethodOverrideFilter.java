package org.sitenetsoft.quarkus.tus.runtime;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Locale;
import java.util.Set;

/**
 * Applies the {@code X-HTTP-Method-Override} header required by the TUS core protocol, so
 * that clients behind proxies which block PATCH and DELETE can still complete an upload.
 * <p>
 * Runs {@link PreMatching} so the rewritten method drives resource matching; every filter
 * that inspects the method — including authentication and rate limiting — therefore sees the
 * effective method rather than the one on the wire.
 */
@Provider
@PreMatching
public class TusMethodOverrideFilter implements ContainerRequestFilter {

    static final String OVERRIDE_HEADER = "X-HTTP-Method-Override";

    private static final Set<String> ALLOWED_METHODS =
            Set.of("GET", "HEAD", "POST", "PATCH", "DELETE", "OPTIONS");

    @ConfigProperty(name = "quarkus.tus.method-override-enabled", defaultValue = "true")
    boolean methodOverrideEnabled;

    @ConfigProperty(name = "quarkus.tus.version", defaultValue = "1.0.0")
    String tusVersion;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (!methodOverrideEnabled) {
            return;
        }

        String override = requestContext.getHeaderString(OVERRIDE_HEADER);
        if (override == null || override.isBlank()) {
            return;
        }

        // Scoped to the TUS endpoints: this header is a TUS requirement and must not change
        // how the rest of the application dispatches requests.
        if (!TusUtils.isTusPath(requestContext.getUriInfo().getPath())) {
            return;
        }

        String method = override.trim().toUpperCase(Locale.ROOT);
        if (!ALLOWED_METHODS.contains(method)) {
            requestContext.abortWith(
                    Response.status(Response.Status.BAD_REQUEST)
                            .header("Tus-Resumable", tusVersion)
                            .entity("Unsupported X-HTTP-Method-Override value: " + override)
                            .build()
            );
            return;
        }

        requestContext.setMethod(method);
    }
}
