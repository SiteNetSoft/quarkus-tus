package org.sitenetsoft.quarkus.tus.client.runtime;

import java.time.Duration;
import java.util.Optional;

/**
 * Client-wide defaults for a {@link TusClient}, mirroring {@code TusClientRuntimeConfig} for
 * programmatic (non-CDI) construction. Any of these can be overridden per upload on
 * {@link TusUploadRequest}.
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

    private TusClientOptions(Builder builder) {
        this.url = builder.url;
        this.chunkSize = builder.chunkSize;
        this.checksumAlgorithm = builder.checksumAlgorithm;
        this.maxRetries = builder.maxRetries;
        this.retryBackoff = builder.retryBackoff;
        this.retryBackoffMax = builder.retryBackoffMax;
        this.parallelism = builder.parallelism;
        this.connectTimeout = builder.connectTimeout;
        this.requestTimeout = builder.requestTimeout;
        this.customizer = builder.customizer;
    }

    public static Builder builder(String url) {
        return new Builder(url);
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

        public TusClientOptions build() {
            return new TusClientOptions(this);
        }
    }
}
