package org.sitenetsoft.quarkus.tus.client.runtime;

import java.time.Duration;
import java.util.Optional;

/**
 * Everything the protocol client needs to know about one TUS server: its endpoint URL, timeouts,
 * and an optional request customizer.
 */
public final class TusTarget {

    private final String url;
    private final Duration connectTimeout;
    private final Duration requestTimeout;
    private final TusRequestCustomizer customizer;

    private TusTarget(Builder builder) {
        this.url = builder.url;
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
        private Duration connectTimeout;
        private Duration requestTimeout;
        private TusRequestCustomizer customizer;

        private Builder(String url) {
            this.url = url;
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

        public TusTarget build() {
            return new TusTarget(this);
        }
    }
}
