package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Options for a TUS creation request (POST). All optional: a plain "creation" request has none
 * set beyond what's needed; "creation-with-upload" additionally streams a body.
 */
public final class TusCreateOptions {

    private final OptionalLong length;
    private final boolean deferLength;
    private final Map<String, String> metadata;
    private final boolean partial;
    private final Multi<Buffer> body;
    private final long bodyLength;

    private TusCreateOptions(Builder builder) {
        this.length = builder.length == null ? OptionalLong.empty() : OptionalLong.of(builder.length);
        this.deferLength = builder.deferLength;
        this.metadata = builder.metadata;
        this.partial = builder.partial;
        this.body = builder.body;
        this.bodyLength = builder.bodyLength;
    }

    public static Builder builder() {
        return new Builder();
    }

    public OptionalLong length() {
        return length;
    }

    public boolean deferLength() {
        return deferLength;
    }

    public Map<String, String> metadata() {
        return metadata;
    }

    public boolean partial() {
        return partial;
    }

    public Optional<Multi<Buffer>> body() {
        return Optional.ofNullable(body);
    }

    public long bodyLength() {
        return bodyLength;
    }

    public static final class Builder {
        private Long length;
        private boolean deferLength;
        private Map<String, String> metadata = Map.of();
        private boolean partial;
        private Multi<Buffer> body;
        private long bodyLength;

        private Builder() {
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder deferLength() {
            this.deferLength = true;
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder partial() {
            this.partial = true;
            return this;
        }

        public Builder body(Multi<Buffer> body, long bodyLength) {
            this.body = body;
            this.bodyLength = bodyLength;
            return this;
        }

        public TusCreateOptions build() {
            return new TusCreateOptions(this);
        }
    }
}
