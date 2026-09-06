package org.sitenetsoft.quarkus.tus.client.runtime;

import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadProgress;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;

/**
 * One upload request: the bytes to send plus everything about how to send them. Anything left
 * unset here falls back to the owning {@link TusClient}'s {@link TusClientOptions} defaults.
 */
public final class TusUploadRequest {

    private final UploadSource source;
    private final Map<String, String> metadata;
    private final Long chunkSize;
    private final String checksumAlgorithm;
    private final Integer parallelism;
    private final Consumer<TusUploadProgress> onProgress;

    private TusUploadRequest(Builder builder) {
        this.source = builder.source;
        this.metadata = builder.metadata;
        this.chunkSize = builder.chunkSize == null
                ? null
                : TusClientOptions.validateChunkSize(builder.chunkSize, "chunkSize");
        this.checksumAlgorithm = builder.checksumAlgorithm;
        this.parallelism = builder.parallelism;
        this.onProgress = builder.onProgress;
    }

    public static Builder builder(UploadSource source) {
        return new Builder(source);
    }

    public UploadSource source() {
        return source;
    }

    public Map<String, String> metadata() {
        return metadata;
    }

    public OptionalLong chunkSize() {
        return chunkSize == null ? OptionalLong.empty() : OptionalLong.of(chunkSize);
    }

    public Optional<String> checksumAlgorithm() {
        return Optional.ofNullable(checksumAlgorithm);
    }

    public OptionalInt parallelism() {
        return parallelism == null ? OptionalInt.empty() : OptionalInt.of(parallelism);
    }

    public Optional<Consumer<TusUploadProgress>> onProgress() {
        return Optional.ofNullable(onProgress);
    }

    public static final class Builder {
        private final UploadSource source;
        private Map<String, String> metadata = Map.of();
        private Long chunkSize;
        private String checksumAlgorithm;
        private Integer parallelism;
        private Consumer<TusUploadProgress> onProgress;

        private Builder(UploadSource source) {
            this.source = source;
        }

        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder chunkSize(long chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder checksumAlgorithm(String checksumAlgorithm) {
            this.checksumAlgorithm = checksumAlgorithm;
            return this;
        }

        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public Builder onProgress(Consumer<TusUploadProgress> onProgress) {
            this.onProgress = onProgress;
            return this;
        }

        public TusUploadRequest build() {
            return new TusUploadRequest(this);
        }
    }
}
