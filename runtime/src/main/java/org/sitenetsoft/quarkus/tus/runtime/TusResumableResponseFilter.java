package org.sitenetsoft.quarkus.tus.runtime;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Guarantees the {@code Tus-Resumable} header on TUS responses.
 * <p>
 * The core protocol requires it on every response except OPTIONS. Resource methods set it
 * themselves, but responses the container produces never reach them — a media-type mismatch on
 * PATCH, an unsupported method, an unmatched path under the TUS prefix — so those went out
 * without it. This fills only the gaps: a header a resource already set is left alone, since it
 * may legitimately differ from the configured version.
 */
@Provider
public class TusResumableResponseFilter implements ContainerResponseFilter {

    static final String TUS_RESUMABLE = "Tus-Resumable";

    @ConfigProperty(name = "quarkus.tus.version", defaultValue = "1.0.0")
    String tusVersion;

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        if (!TusUtils.isTusPath(requestContext.getUriInfo().getPath())) {
            return;
        }
        // The spec exempts OPTIONS; the capability response sets the header on its own anyway.
        if ("OPTIONS".equalsIgnoreCase(requestContext.getMethod())) {
            return;
        }
        if (!responseContext.getHeaders().containsKey(TUS_RESUMABLE)) {
            responseContext.getHeaders().add(TUS_RESUMABLE, tusVersion);
        }
    }
}
