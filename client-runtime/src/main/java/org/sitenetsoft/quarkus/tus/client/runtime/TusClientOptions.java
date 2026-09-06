package org.sitenetsoft.quarkus.tus.client.runtime;

import java.time.Duration;
import java.util.Optional;

/**
 * Client-wide defaults for a {@link TusClient}, mirroring {@code TusClientRuntimeConfig} for
 * programmatic (non-CDI) construction. Chunk size, checksum algorithm, parallelism and the progress
 * callback can be overridden per upload on {@link TusUploadRequest}; the URL, retry policy, timeouts
 * and customizer are per client.
 */
public final class TusClientOptions {

    private final String url;
    private final long chunkSize;
    private final String checksumAlgorithm;
    private final int maxRetries;
    private final Duration retryBackoff;
    private final Duration retryBackoffMax;
    private final int parallelism;
    private final Duration connectTimeout;
    private final Duration requestTimeout;
    private final TusRequestCustomizer customizer;
    private final TusHttpClientCustomizer httpClientOptions;

    private TusClientOptions(Builder builder) {
        this.url = builder.url;
        this.chunkSize = validateChunkSize(builder.chunkSize, "chunkSize");
        this.checksumAlgorithm = builder.checksumAlgorithm;
        this.maxRetries = builder.maxRetries;
        this.retryBackoff = builder.retryBackoff;
        this.retryBackoffMax = builder.retryBackoffMax;
        this.parallelism = builder.parallelism;
        this.connectTimeout = builder.connectTimeout;
        this.requestTimeout = builder.requestTimeout;
        this.customizer = builder.customizer;
        this.httpClientOptions = builder.httpClientOptions;
    }

    public static Builder builder(String url) {
        return new Builder(url);
    }

    /**
     * A chunk size must be at least 1 byte (0 would never advance the chunk loop) and at most
     * {@link Integer#MAX_VALUE} (the checksum and one-shot paths collect one chunk into a single
     * Vert.x {@code Buffer}, which is {@code int}-indexed). {@code what} names the offending setting
     * in the message -- the builder property or the config key.
     */
    static long validateChunkSize(long chunkSize, String what) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException(what + " must be greater than 0, was " + chunkSize);
        }
        if (chunkSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(what + " must not exceed " + Integer.MAX_VALUE
                    + " bytes (one chunk must fit in a single buffer), was " + chunkSize);
        }
        return chunkSize;
    }

    public String url() {
        return url;
    }

    public long chunkSize() {
        return chunkSize;
    }

    public Optional<String> checksumAlgorithm() {
        return Optional.ofNullable(checksumAlgorithm);
    }

    public int maxRetries() {
        return maxRetries;
    }

    public Duration retryBackoff() {
        return retryBackoff;
    }

    public Duration retryBackoffMax() {
        return retryBackoffMax;
    }

    public int parallelism() {
        return parallelism;
    }

    public Optional<Duration> connectTimeout() {
        return Optional.ofNullable(connectTimeout);
    }

    public Optional<Duration> requestTimeout() {
        return Optional.ofNullable(requestTimeout);
    }

    public Optional<TusRequestCustomizer> customizer() {
        return Optional.ofNullable(customizer);
    }

    public Optional<TusHttpClientCustomizer> httpClientOptions() {
        return Optional.ofNullable(httpClientOptions);
    }

    public static final class Builder {
        private final String url;
        private long chunkSize = 10485760L;
        private String checksumAlgorithm;
        private int maxRetries = 3;
        private Duration retryBackoff = Duration.ofSeconds(1);
        private Duration retryBackoffMax = Duration.ofSeconds(30);
        private int parallelism = 1;
        private Duration connectTimeout;
        private Duration requestTimeout;
        private TusRequestCustomizer customizer;
        private TusHttpClientCustomizer httpClientOptions;

        private Builder(String url) {
            this.url = url;
        }

        public Builder chunkSize(long chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder checksumAlgorithm(String checksumAlgorithm) {
            this.checksumAlgorithm = checksumAlgorithm;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryBackoff(Duration retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        public Builder retryBackoffMax(Duration retryBackoffMax) {
            this.retryBackoffMax = retryBackoffMax;
            return this;
        }

        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder customizer(TusRequestCustomizer customizer) {
            this.customizer = customizer;
            return this;
        }

        /**
         * Configures the Vert.x {@link io.vertx.core.http.HttpClientOptions} the client's HTTP
         * client is built from (TLS trust, proxy, pool size, ...). See {@link TusHttpClientCustomizer}.
         */
        public Builder httpClientOptions(TusHttpClientCustomizer httpClientOptions) {
            this.httpClientOptions = httpClientOptions;
            return this;
        }

        public TusClientOptions build() {
            return new TusClientOptions(this);
        }
    }
}
