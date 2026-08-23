package org.sitenetsoft.quarkus.tus.client.runtime.config;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;
import java.util.Optional;

@ConfigMapping(prefix = "quarkus.tus.client")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface TusClientRuntimeConfig {

    /**
     * Base URL of the TUS server (optional).
     */
    Optional<String> url();

    /**
     * Chunk size for uploads in bytes.
     */
    @WithDefault("10485760")
    long chunkSize();

    /**
     * Checksum algorithm to use (optional).
     */
    Optional<String> checksumAlgorithm();

    /**
     * Maximum number of retries for failed requests.
     */
    @WithDefault("3")
    int maxRetries();

    /**
     * Initial backoff duration for retries.
     */
    @WithDefault("1S")
    Duration retryBackoff();

    /**
     * Maximum backoff duration for retries.
     */
    @WithDefault("30S")
    Duration retryBackoffMax();

    /**
     * Number of parallel upload streams.
     */
    @WithDefault("1")
    int parallelism();

    /**
     * Connection timeout (optional).
     */
    Optional<Duration> connectTimeout();

    /**
     * Request timeout (optional).
     */
    Optional<Duration> requestTimeout();
}
