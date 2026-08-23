package org.sitenetsoft.quarkus.tus.client.runtime;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Options for a TUS PATCH request. All optional: a bare chunk upload needs none of them.
 */
public final class TusPatchOptions {

    private final OptionalLong contentLength;
    private final String checksumAlgorithm;
    private final String checksumDigest;
    private final OptionalLong declaredUploadLength;

    private TusPatchOptions(Builder builder) {
        this.contentLength = builder.contentLength == null ? OptionalLong.empty() : OptionalLong.of(builder.contentLength);
        this.checksumAlgorithm = builder.checksumAlgorithm;
        this.checksumDigest = builder.checksumDigest;
        this.declaredUploadLength = builder.declaredUploadLength == null ? OptionalLong.empty()
                : OptionalLong.of(builder.declaredUploadLength);
    }

    public static TusPatchOptions none() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public OptionalLong contentLength() {
        return contentLength;
    }

    public Optional<String> checksumAlgorithm() {
        return Optional.ofNullable(checksumAlgorithm);
    }

    public Optional<String> checksumDigest() {
        return Optional.ofNullable(checksumDigest);
    }

    public OptionalLong declaredUploadLength() {
        return declaredUploadLength;
    }

    public static final class Builder {
        private Long contentLength;
        private String checksumAlgorithm;
        private String checksumDigest;
        private Long declaredUploadLength;

        private Builder() {
        }

        public Builder contentLength(long contentLength) {
            this.contentLength = contentLength;
            return this;
        }

        public Builder checksum(String algorithm, String base64Digest) {
            this.checksumAlgorithm = algorithm;
            this.checksumDigest = base64Digest;
            return this;
        }

        public Builder declareUploadLength(long length) {
            this.declaredUploadLength = length;
            return this;
        }

        public TusPatchOptions build() {
            return new TusPatchOptions(this);
        }
    }
}
