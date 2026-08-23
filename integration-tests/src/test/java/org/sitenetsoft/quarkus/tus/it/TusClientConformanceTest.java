package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.client.runtime.TusClient;
import org.sitenetsoft.quarkus.tus.client.runtime.TusClientOptions;
import org.sitenetsoft.quarkus.tus.client.runtime.TusCreateOptions;
import org.sitenetsoft.quarkus.tus.client.runtime.TusPatchOptions;
import org.sitenetsoft.quarkus.tus.client.runtime.TusUploadRequest;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusChecksumMismatchException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusUploadNotFoundException;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusServerCapabilities;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUpload;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadResult;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Mutual conformance: our own TUS client driving our own in-repo TUS server over real HTTP, on
 * each test's own {@link TusClient} so it can pick its own chunk size, checksum algorithm, retry
 * budget and parallelism. The server under test runs with {@code max-chunk-size=1024}
 * (integration-tests' {@code application.properties}), so every client here keeps its chunk size
 * at or below that.
 */
@QuarkusTest
class TusClientConformanceTest {

    @Inject
    io.vertx.core.Vertx vertx;

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    private final List<TusClient> createdClients = new ArrayList<>();

    @AfterEach
    void closeClients() {
        for (TusClient client : createdClients) {
            client.close();
        }
        createdClients.clear();
    }

    private String baseUrl() {
        return "http://localhost:" + RestAssured.port + "/tus";
    }

    private TusClient newClient(TusClientOptions.Builder builder) {
        TusClient client = TusClient.create(vertx, builder.build());
        createdClients.add(client);
        return client;
    }

    private TusClient newClient() {
        return newClient(TusClientOptions.builder(baseUrl()));
    }

    private String uploadIdFor(String uploadUrl) {
        int slash = uploadUrl.lastIndexOf('/');
        return uploadUrl.substring(slash + 1);
    }

    private byte[] storedBytes(String uploadUrl) throws IOException {
        Path dataFile = Path.of(tusRuntimeConfig.store().local().uploadDir(), uploadIdFor(uploadUrl));
        return Files.readAllBytes(dataFile);
    }

    private static byte[] randomBytes(int length) {
        byte[] data = new byte[length];
        new Random(42).nextBytes(data);
        return data;
    }

    @Test
    void sequentialUploadArrivesByteForByte() throws IOException {
        byte[] data = randomBytes(3000);
        Path file = Files.createTempFile("tus-client-conformance-seq", ".bin");
        Files.write(file, data);

        TusClient client = newClient(TusClientOptions.builder(baseUrl()).chunkSize(512));
        TusUploadResult result = client.upload(TusUploadRequest.builder(UploadSource.ofFile(vertx, file)).build())
                .await().atMost(Duration.ofSeconds(20));

        assertEquals(3000, result.bytesUploaded());
        assertArrayEquals(data, storedBytes(result.url()));
    }

    @Test
    void resumeAfterAStreamKilledMidChunk() throws IOException {
        byte[] data = randomBytes(1500);
        UploadSource flaky = flakySourceFailingOnce(data, 700);

        TusClient client = newClient(TusClientOptions.builder(baseUrl())
                .chunkSize(1024)
                .maxRetries(3)
                .retryBackoff(Duration.ofMillis(10))
                .retryBackoffMax(Duration.ofMillis(50))
                // The simulated break just stops writing mid-body without resetting the
                // connection, so nothing signals failure at the HTTP layer on its own; a request
                // timeout is what turns that hang into the retryable failure this test is after.
                .requestTimeout(Duration.ofSeconds(2)));
        TusUploadResult result = client.upload(TusUploadRequest.builder(flaky).build())
                .await().atMost(Duration.ofSeconds(30));

        assertEquals(1500, result.bytesUploaded());
        assertArrayEquals(data, storedBytes(result.url()));
    }

    /**
     * A replayable {@link UploadSource} over an in-memory array whose very first {@link
     * UploadSource#slice(long)} call fails partway through (after {@code failAtByte} bytes),
     * simulating a connection reset mid-chunk. Every subsequent call succeeds.
     */
    private static UploadSource flakySourceFailingOnce(byte[] data, long failAtByte) {
        AtomicBoolean firstCall = new AtomicBoolean(true);
        return new UploadSource() {
            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Multi<Buffer> slice(long fromOffset) {
                Buffer full = Buffer.buffer(data).getBuffer((int) fromOffset, data.length);
                if (firstCall.compareAndSet(true, false)) {
                    long cut = Math.min(failAtByte, full.length());
                    Buffer before = full.getBuffer(0, (int) cut);
                    return Multi.createFrom().emitter(emitter -> {
                        emitter.emit(before);
                        emitter.fail(new RuntimeException("simulated stream break mid-chunk"));
                    });
                }
                return Multi.createFrom().item(full);
            }

            @Override
            public boolean replayable() {
                return true;
            }
        };
    }

    @Test
    void checksumGoodAndBad() throws IOException, NoSuchAlgorithmException {
        // Good: high-level upload with sha1, replayable file source, chunk within server limit.
        byte[] data = randomBytes(900);
        Path file = Files.createTempFile("tus-client-conformance-checksum", ".bin");
        Files.write(file, data);

        TusClient goodClient = newClient(TusClientOptions.builder(baseUrl()).chunkSize(1024).checksumAlgorithm("sha1"));
        TusUploadResult result = goodClient.upload(TusUploadRequest.builder(UploadSource.ofFile(vertx, file)).build())
                .await().atMost(Duration.ofSeconds(20));

        assertEquals(900, result.bytesUploaded());
        assertArrayEquals(data, storedBytes(result.url()));

        // Bad: low-level PATCH with a deliberately wrong digest.
        TusClient lowLevelClient = newClient();
        TusUpload created = lowLevelClient.protocol().create(TusCreateOptions.builder().length(100).build())
                .await().atMost(Duration.ofSeconds(10));

        byte[] chunk = randomBytes(100);
        String wrongDigest = Base64.getEncoder().encodeToString(
                MessageDigest.getInstance("SHA-1").digest("not the right bytes".getBytes()));
        TusPatchOptions badPatch = TusPatchOptions.builder()
                .contentLength(100)
                .checksum("sha1", wrongDigest)
                .build();

        assertThrows(TusChecksumMismatchException.class, () -> lowLevelClient.protocol()
                .patch(created.url(), 0, Multi.createFrom().item(Buffer.buffer(chunk)), badPatch)
                .await().atMost(Duration.ofSeconds(10)));

        long offsetAfterFailure = lowLevelClient.protocol().offset(created.url()).await().atMost(Duration.ofSeconds(10));
        assertEquals(0L, offsetAfterFailure, "a failed checksum must not advance the server's offset");
    }

    @Test
    void deferLengthEndsWithTheDeclaringEmptyPatch() throws IOException {
        // >1024 bytes over a 1024 chunk size forces multiple data PATCHes before the source runs
        // dry. Delivered as three items sized so the *last* one never itself fills a full chunk
        // (1024 + 1024 + 452 == 2500, chunk size 1024): uploadDeferLength only asks its upstream
        // for another item once its own pending buffer is below the chunk size, so the final item
        // landing under that threshold guarantees no chunk PATCH is still in flight when the
        // source's own completion signal (which Reactive Streams lets a publisher send the moment
        // it's exhausted, independent of any further demand) reaches the declaring-PATCH logic.
        byte[] data = randomBytes(2500);
        Buffer wholeBody = Buffer.buffer(data);
        Multi<Buffer> body = Multi.createFrom().items(
                wholeBody.getBuffer(0, 1024),
                wholeBody.getBuffer(1024, 2048),
                wholeBody.getBuffer(2048, 2500));

        TusClient client = newClient(TusClientOptions.builder(baseUrl()).chunkSize(1024));
        TusUploadResult result = client.upload(TusUploadRequest.builder(UploadSource.oneShot(body, -1)).build())
                .await().atMost(Duration.ofSeconds(20));

        assertEquals(2500, result.bytesUploaded());
        assertArrayEquals(data, storedBytes(result.url()));

        long headOffset = client.protocol().offset(result.url()).await().atMost(Duration.ofSeconds(10));
        assertEquals(2500L, headOffset);
    }

    @Test
    void parallelConcatenationReassemblesInOrder() throws IOException {
        byte[] data = randomBytes(2500);
        Path file = Files.createTempFile("tus-client-conformance-parallel", ".bin");
        Files.write(file, data);

        TusClient client = newClient(TusClientOptions.builder(baseUrl()).chunkSize(1024).parallelism(3));
        TusUploadResult result = client.upload(TusUploadRequest.builder(UploadSource.ofFile(vertx, file)).build())
                .await().atMost(Duration.ofSeconds(30));

        assertEquals(2500, result.bytesUploaded());
        assertArrayEquals(data, storedBytes(result.url()));
    }

    @Test
    void terminateMakesTheUploadGone() {
        TusClient client = newClient();
        TusUpload created = client.protocol().create(TusCreateOptions.builder().length(10).build())
                .await().atMost(Duration.ofSeconds(10));
        byte[] chunk = randomBytes(10);
        client.protocol().patch(created.url(), 0, Multi.createFrom().item(Buffer.buffer(chunk)),
                        TusPatchOptions.builder().contentLength(10).build())
                .await().atMost(Duration.ofSeconds(10));

        client.protocol().terminate(created.url()).await().atMost(Duration.ofSeconds(10));

        assertThrows(TusUploadNotFoundException.class,
                () -> client.protocol().offset(created.url()).await().atMost(Duration.ofSeconds(10)));
    }

    @Test
    void expirationIsSurfaced() {
        TusClient client = newClient();
        TusUpload created = client.protocol().create(TusCreateOptions.builder().length(50).build())
                .await().atMost(Duration.ofSeconds(10));

        assertTrue(created.expiresAt().isPresent(), "server has expiration enabled, expiresAt must be present");
        assertTrue(created.expiresAt().get().isAfter(Instant.now()), "expiresAt must be in the future");
    }

    @Test
    void capabilitiesReflectTheServer() {
        TusClient client = newClient();
        TusServerCapabilities caps = client.protocol().options().await().atMost(Duration.ofSeconds(10));

        assertTrue(caps.supports("creation"), "creation");
        assertTrue(caps.supports("termination"), "termination");
        assertTrue(caps.supports("checksum"), "checksum");
        assertTrue(caps.supports("concatenation"), "concatenation");
        assertTrue(caps.supports("creation-defer-length"), "creation-defer-length");
    }
}
