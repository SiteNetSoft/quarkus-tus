package org.sitenetsoft.quarkus.tus.runtime.config;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "quarkus.tus")
@ConfigRoot(phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED)
public interface TusBuildTimeConfig {

    /**
     * Whether SSE (Server-Sent Events) endpoints and beans are registered.
     */
    @WithDefault("true")
    boolean sseEnabled();

    /**
     * Whether the authentication filter is registered.
     */
    @WithDefault("false")
    boolean authEnabled();

    /**
     * Whether the rate limit filter is registered.
     */
    @WithDefault("false")
    boolean rateLimitEnabled();

    /**
     * Whether the {@code X-HTTP-Method-Override} filter is registered. Required by the TUS
     * core protocol, so enabled by default; disable it if a proxy in front of the application
     * enforces rules based on the HTTP method.
     */
    @WithDefault("true")
    boolean methodOverrideEnabled();

    /**
     * URL path for TUS endpoints.
     */
    @WithDefault("/tus")
    String path();
}
