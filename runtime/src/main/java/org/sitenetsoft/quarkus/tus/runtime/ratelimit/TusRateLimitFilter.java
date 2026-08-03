package org.sitenetsoft.quarkus.tus.runtime.ratelimit;

import io.quarkus.vertx.http.runtime.CurrentVertxRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.sitenetsoft.quarkus.tus.runtime.TusUtils;

@Provider
@Priority(Priorities.USER + 1)
public class TusRateLimitFilter implements ContainerRequestFilter {

    @ConfigProperty(name = "quarkus.tus.rate-limit-enabled", defaultValue = "false")
    boolean rateLimitEnabled;

    @Inject
    TusRateLimitService rateLimitService;

    @Inject
    CurrentVertxRequest currentVertxRequest;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (!rateLimitEnabled) {
            return;
        }

        // Matched against where the endpoints are actually mounted, never against
        // configuration: a mismatch would silently disable throttling.
        if (!TusUtils.isTusPath(requestContext.getUriInfo().getPath())) {
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

    /**
     * Identifies the caller to throttle: the authenticated principal, otherwise the peer
     * address of the connection.
     * <p>
     * Forwarding headers are deliberately not read here. They are attacker-controlled, so
     * keying on them let a client buy a fresh burst per request simply by varying the value —
     * and grew an unbounded map of buckets while doing it. Behind a real proxy, enable
     * {@code quarkus.http.proxy.proxy-address-forwarding} (with
     * {@code quarkus.http.proxy.trusted-proxies}); Quarkus then resolves the forwarded address
     * into the peer address seen here, having first checked that the proxy is trusted.
     */
    private String resolveClientId(ContainerRequestContext requestContext) {
        if (requestContext.getSecurityContext() != null
                && requestContext.getSecurityContext().getUserPrincipal() != null) {
            return requestContext.getSecurityContext().getUserPrincipal().getName();
        }

        RoutingContext routingContext = currentVertxRequest.getCurrent();
        if (routingContext != null) {
            SocketAddress remote = routingContext.request().remoteAddress();
            if (remote != null && remote.hostAddress() != null) {
                return remote.hostAddress();
            }
        }

        return "unknown";
    }
}
