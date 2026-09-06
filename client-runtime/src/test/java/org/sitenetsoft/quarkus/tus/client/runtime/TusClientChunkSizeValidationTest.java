package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.sitenetsoft.quarkus.tus.client.runtime.config.TusClientRuntimeConfig;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A chunk size of zero would spin the chunk loop forever; a negative one is nonsense; and one past
 * {@code Integer.MAX_VALUE} can't be held in a single Vert.x {@link Buffer} (the checksum and
 * one-shot paths collect a chunk into one). All three entry points -- the programmatic options,
 * the config-backed producer at boot, and the per-request override -- must reject them up front
 * with a message that says what was wrong.
 */
class TusClientChunkSizeValidationTest {

    private static final long TOO_BIG = (long) Integer.MAX_VALUE + 1;

    private static UploadSource source() {
        return UploadSource.oneShot(Multi.createFrom().item(Buffer.buffer("x")), 1);
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1, TOO_BIG })
    void optionsBuilderRejectsInvalidChunkSizes(long chunkSize) {
        var e = assertThrows(IllegalArgumentException.class,
                () -> TusClientOptions.builder("http://localhost/tus").chunkSize(chunkSize).build());
        assertTrue(e.getMessage().contains("chunkSize") && e.getMessage().contains(Long.toString(chunkSize)),
                "message should name chunkSize and the offending value: " + e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1, TOO_BIG })
    void uploadRequestBuilderRejectsInvalidChunkSizes(long chunkSize) {
        var e = assertThrows(IllegalArgumentException.class,
                () -> TusUploadRequest.builder(source()).chunkSize(chunkSize).build());
        assertTrue(e.getMessage().contains("chunkSize") && e.getMessage().contains(Long.toString(chunkSize)),
                "message should name chunkSize and the offending value: " + e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1, TOO_BIG })
    void bootValidationRejectsInvalidConfiguredChunkSizes(long chunkSize) {
        var e = assertThrows(IllegalArgumentException.class,
                () -> new TusClientProducer().validateConfig(null, configWithChunkSize(chunkSize)));
        assertTrue(e.getMessage().contains("quarkus.tus.client.chunk-size")
                        && e.getMessage().contains(Long.toString(chunkSize)),
                "message should name the property and the offending value: " + e.getMessage());
    }

    @Test
    void theBoundariesAreAccepted() {
        assertDoesNotThrow(() -> TusClientOptions.builder("http://localhost/tus").chunkSize(1).build());
        assertDoesNotThrow(() -> TusClientOptions.builder("http://localhost/tus").chunkSize(Integer.MAX_VALUE).build());
        assertDoesNotThrow(() -> TusUploadRequest.builder(source()).chunkSize(1).build());
        assertDoesNotThrow(() -> new TusClientProducer().validateConfig(null, configWithChunkSize(Integer.MAX_VALUE)));
        assertEquals(1, TusUploadRequest.builder(source()).chunkSize(1).build().chunkSize().getAsLong());
    }

    private static TusClientRuntimeConfig configWithChunkSize(long chunkSize) {
        return new TusClientRuntimeConfig() {
            @Override
            public Optional<String> url() {
                return Optional.of("http://localhost/tus");
            }

            @Override
            public long chunkSize() {
                return chunkSize;
            }

            @Override
            public Optional<String> checksumAlgorithm() {
                return Optional.empty();
            }

            @Override
            public int maxRetries() {
                return 3;
            }

            @Override
            public Duration retryBackoff() {
                return Duration.ofSeconds(1);
            }

            @Override
            public Duration retryBackoffMax() {
                return Duration.ofSeconds(30);
            }

            @Override
            public int parallelism() {
                return 1;
            }

            @Override
            public Optional<Duration> connectTimeout() {
                return Optional.empty();
            }

            @Override
            public Optional<Duration> requestTimeout() {
                return Optional.empty();
            }
        };
    }
}
