package org.sitenetsoft.quarkus.tus.client.runtime;

import io.vertx.core.http.HttpClientOptions;

/**
 * Hook to configure the Vert.x {@link HttpClientOptions} the client's HTTP client is built from,
 * once, at client creation -- TLS trust for a private CA, an egress proxy, connection pool sizing,
 * and so on. Programmatically, pass it through {@link TusClientOptions.Builder#httpClientOptions};
 * under CDI, expose exactly one bean of this type and the injected {@link TusClient} picks it up
 * (like {@link TusRequestCustomizer}, a second bean makes the client an unavailable shim rather
 * than choosing one arbitrarily).
 *
 * <p>The connect timeout from {@code TusClientOptions}/{@code quarkus.tus.client.connect-timeout} is
 * applied before this hook runs, so the hook can override it.
 */
@FunctionalInterface
public interface TusHttpClientCustomizer {
    void customize(HttpClientOptions options);
}
