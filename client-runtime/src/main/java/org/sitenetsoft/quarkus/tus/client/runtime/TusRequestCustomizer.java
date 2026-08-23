package org.sitenetsoft.quarkus.tus.client.runtime;

import io.vertx.core.MultiMap;

/**
 * Hook to add or override headers on every request the client sends, e.g. for authentication.
 */
public interface TusRequestCustomizer {
    void customize(String method, String url, MultiMap headers);
}
