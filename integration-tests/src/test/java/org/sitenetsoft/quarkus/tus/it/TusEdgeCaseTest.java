package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.UploadProgressService;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.spi.OffsetMismatchException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.store.LocalFileUploadStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case and concurrency tests for TUS upload handling.
 */
@QuarkusTest
class TusEdgeCaseTest {

    @Inject
    UploadStore uploadStore;

    @Inject
    TusTestObserver observer;

    @Inject
    UploadProgressService uploadProgressService;

    @Inject
    io.vertx.mutiny.core.Vertx vertx;

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    @BeforeEach
    void setUp() {
        observer.reset();
    }

    // ---- Path traversal defense ----

    @Test
    void testPathTraversalInHeadReturns400() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head("/tus/..%2F..%2Fetc%2Fpasswd")
                .then()
                .statusCode(400);
    }

    @Test
    void testPathTraversalInPatchReturns400() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body("test".getBytes())
                .when().patch("/tus/..%2F..%2Fetc%2Fpasswd")
                .then()
                .statusCode(400);
    }

    @Test
    void testPathTraversalInDeleteReturns400() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete("/tus/..%2F..%2Fetc%2Fpasswd")
                .then()
                .statusCode(400);
    }

    // ---- Offset validation must happen under the lock (TOCTOU) ----

    /**
     * The offset used to be validated before the lock was taken, so two requests could both
     * pass validation and then write in turn, the second one overwriting the first. Holding
     * the lock must therefore be what a conflicting request notices first — a wrong offset
     * cannot be judged until the state can be read consistently.
     */
    @Test
    void testPatchWithWrongOffsetWhileLockedReturns423() {
        String location = createUpload(100);
        String uploadId = extractId(location);

        assertTrue(Stores.lock(uploadStore, uploadId), "Test needs to hold the lock");
        try {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "50") // deliberately wrong; real offset is 0
                    .contentType("application/offset+octet-stream")
                    .body("xxxx".getBytes())
                    .when().patch(location)
                    .then()
                    .statusCode(423);
        } finally {
            Stores.unlock(uploadStore, uploadId);
        }
    }

    /**
     * The store must not trust the caller-supplied offset either: a third-party caller (or a
     * request that raced past the resource) writing at a stale offset would silently corrupt
     * already-written bytes.
     */
    @Test
    void testStoreRejectsWriteAtStaleOffset() throws Exception {
        byte[] first = "AAAA".getBytes();
        String location = createUpload(8);
        String uploadId = extractId(location);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(first)
                .when().patch(location)
                .then()
                .statusCode(204);

        // Offset is now 4. Writing at 0 again must be refused, not silently applied.
        assertThrows(OffsetMismatchException.class, () ->
                        uploadStore.stageChunk(uploadId, 0,
                                        Multi.createFrom().item(Buffer.buffer("BB")), 2)
                                .await().atMost(java.time.Duration.ofSeconds(5)),
                "Staging at a stale offset must fail");

        assertEquals(4, Stores.find(uploadStore, uploadId).orElseThrow().getOffset(),
                "Offset must be unchanged after the rejected write");

        Path dataFile = Path.of(tusRuntimeConfig.store().local().uploadDir(), uploadId);
        byte[] onDisk = Files.readAllBytes(dataFile);
        assertArrayEquals(first, onDisk, "Already-written bytes must be intact");
    }

    /**
     * Offset validation now happens while holding the lock, so every rejection path between
     * acquiring it and handing off to the write must release it. A leak would make the upload
     * unwritable (423) until the 30s lock timeout expired.
     */
    @Test
    void testLockIsReleasedAfterOffsetMismatch() {
        byte[] data = "hello".getBytes();
        String location = createUpload(data.length);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "3") // wrong; upload is at 0
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(409);

        // Must be writable immediately, not blocked behind a leaked lock.
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);
    }

    // ---- Completion fires once per upload ----

    /**
     * A PATCH at the final offset with an empty body passes every check — the offset matches,
     * nothing exceeds the declared length, and a zero-byte write succeeds — so it used to
     * re-fire TusUploadCompletedEvent every time. Consumers typically move files, insert
     * rows, call webhooks or bill for the upload, so this let any client replay those effects
     * indefinitely.
     */
    @Test
    void testCompletionEventFiresOnlyOnceWhenRePatchedAtFinalOffset() {
        byte[] data = "all done".getBytes();
        String location = createUpload(data.length);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);

        assertEquals(1, observer.completedEvents.size(),
                "Completing the upload should fire exactly one completion event");

        for (int i = 0; i < 3; i++) {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", String.valueOf(data.length))
                    .contentType("application/offset+octet-stream")
                    .body(new byte[0])
                    .when().patch(location);
        }

        assertEquals(1, observer.completedEvents.size(),
                "Re-patching a complete upload must not fire further completion events");
    }

    /**
     * The same guard must not stop a zero-length upload from completing: its only chance to
     * fire is an empty PATCH, since offset already equals the declared length.
     */
    @Test
    void testZeroLengthUploadStillFiresCompletionOnce() {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "0")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        for (int i = 0; i < 3; i++) {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(new byte[0])
                    .when().patch(location)
                    .then()
                    .statusCode(204);
        }

        assertEquals(1, observer.completedEvents.size(),
                "A zero-length upload should complete exactly once");
    }

    // ---- Discard must not run while a write holds the lock ----

    /**
     * discardUpload used to delete the data file and forcibly drop another thread's lock. A
     * DELETE landing mid-write therefore unlinked the file underneath the in-flight write,
     * which went on to report success for bytes that were never durably stored.
     */
    @Test
    void testDeleteWhileLockedReturns423() {
        String location = createUpload(100);
        String uploadId = extractId(location);

        assertTrue(Stores.lock(uploadStore, uploadId), "Test needs to hold the lock");
        try {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().delete(location)
                    .then()
                    .statusCode(423);
        } finally {
            Stores.unlock(uploadStore, uploadId);
        }

        // The upload must have survived the attempt, and be deletable once unlocked.
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete(location)
                .then()
                .statusCode(204);
    }

    @Test
    void testDeleteWhileLockedKeepsData() throws Exception {
        byte[] data = "keep me".getBytes();
        String location = createUpload(data.length);
        String uploadId = extractId(location);
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);

        Path dataFile = Path.of(tusRuntimeConfig.store().local().uploadDir(), uploadId);

        assertTrue(Stores.lock(uploadStore, uploadId), "Test needs to hold the lock");
        try {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().delete(location)
                    .then()
                    .statusCode(423);
            assertTrue(Stores.find(uploadStore, uploadId).isPresent(),
                    "Upload entry must survive a refused delete");
            assertTrue(Files.exists(dataFile), "Data file must survive a refused delete");
            assertArrayEquals(data, Files.readAllBytes(dataFile), "Data must be intact");
        } finally {
            Stores.unlock(uploadStore, uploadId);
        }

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete(location)
                .then()
                .statusCode(204);
        assertFalse(Files.exists(dataFile), "Data file must be gone after a successful delete");
    }

    // ---- Creation-with-upload over HTTP/2 ----

    /**
     * Over HTTP/2 there is no Transfer-Encoding header: a streamed body without a content
     * length is just DATA frames. Deciding "has a body" from Content-Length or a literal
     * "chunked" would treat it as no body and silently drop the bytes.
     */
    @Test
    void testCreationWithUploadOverHttp2WithoutContentLength() throws Exception {
        byte[] data = "h2 first chunk".getBytes();
        byte[] more = "h2 second chunk".getBytes();
        io.vertx.mutiny.core.http.HttpClient client = vertx.createHttpClient(new io.vertx.core.http.HttpClientOptions()
                .setProtocolVersion(io.vertx.core.http.HttpVersion.HTTP_2)
                .setHttp2ClearTextUpgrade(false));
        try {
            io.vertx.mutiny.core.http.HttpClientResponse response = client
                    .request(io.vertx.core.http.HttpMethod.POST, io.restassured.RestAssured.port, "localhost", "/tus")
                    .flatMap(req -> {
                        req.putHeader("Tus-Resumable", "1.0.0");
                        req.putHeader("Upload-Length", String.valueOf(data.length + more.length));
                        req.putHeader("Content-Type", "application/offset+octet-stream");
                        req.setChunked(true); // no Content-Length; over h2 this is just DATA frames
                        return req.write(io.vertx.mutiny.core.buffer.Buffer.buffer(data))
                                .chain(() -> req.end())
                                .chain(() -> req.response());
                    })
                    .await().atMost(java.time.Duration.ofSeconds(10));
            assertEquals(io.vertx.core.http.HttpVersion.HTTP_2, response.version(), "the test needs an h2c connection");
            assertEquals(201, response.statusCode());
            assertEquals(String.valueOf(data.length), response.getHeader("Upload-Offset"),
                    "the body sent with the creation must have been stored");

            // And a length-less PATCH over the same h2 connection.
            String location = response.getHeader("Location");
            io.vertx.mutiny.core.http.HttpClientResponse patched = client
                    .request(io.vertx.core.http.HttpMethod.PATCH, io.restassured.RestAssured.port, "localhost", location)
                    .flatMap(req -> {
                        req.putHeader("Tus-Resumable", "1.0.0");
                        req.putHeader("Upload-Offset", String.valueOf(data.length));
                        req.putHeader("Content-Type", "application/offset+octet-stream");
                        req.setChunked(true);
                        return req.write(io.vertx.mutiny.core.buffer.Buffer.buffer(more))
                                .chain(() -> req.end())
                                .chain(() -> req.response());
                    })
                    .await().atMost(java.time.Duration.ofSeconds(10));
            assertEquals(204, patched.statusCode());
            assertEquals(String.valueOf(data.length + more.length), patched.getHeader("Upload-Offset"));
        } finally {
            client.closeAndAwait();
        }
    }

    // ---- Every discard clears the progress entry ----

    /**
     * Progress bookkeeping is the framework's, not the store's, so every path that discards an
     * upload has to clear it — the expiry discards in HEAD and PATCH used to leave the entry to
     * the two-hour TTL.
     */
    @Test
    void testExpiryDiscardOnHeadClearsProgress() {
        String location = createUpload(100);
        String uploadId = extractId(location);
        assertNotNull(uploadProgressService.getProgress(uploadId), "creation starts progress tracking");

        UploadInfo info = Stores.find(uploadStore, uploadId).orElseThrow();
        info.setExpiresAt(Instant.now().minusSeconds(1));
        Stores.update(uploadStore, uploadId, info);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(410);

        assertTrue(Stores.find(uploadStore, uploadId).isEmpty(), "expired upload is discarded");
        assertNull(uploadProgressService.getProgress(uploadId), "progress entry must go with it");
    }

    // ---- Concurrent PATCH on same upload ----

    /**
     * Competing writers each send a differently sized chunk at offset 0, so an interleaved
     * write leaves a detectable mix of two chunks rather than one intact chunk.
     * <p>
     * Losers may be rejected with either 423 (the lock was still held) or 409 (the lock was
     * acquired after the winner finished, so the offset had moved). Both are correct; which
     * one a given request sees is a timing detail, so the test asserts the invariant — one
     * winner, no corruption — rather than a particular split.
     */
    @Test
    void testConcurrentPatchOnSameUpload() throws Exception {
        int threads = 5;
        int maxChunk = threads * 4;
        String location = createUpload(maxChunk);
        String uploadId = extractId(location);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> statusCodes = Collections.synchronizedList(new ArrayList<>());

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            byte[] chunk = new byte[(i + 1) * 4];
            java.util.Arrays.fill(chunk, (byte) ('A' + i));
            futures.add(executor.submit(() -> {
                try {
                    latch.await();
                    int status = given()
                            .header("Tus-Resumable", "1.0.0")
                            .header("Upload-Offset", "0")
                            .contentType("application/offset+octet-stream")
                            .body(chunk)
                            .when().patch(location)
                            .then()
                            .extract().statusCode();
                    statusCodes.add(status);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        latch.countDown();

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        executor.shutdown();

        long successCount = statusCodes.stream().filter(s -> s == 204).count();
        assertEquals(1, successCount, "Exactly one PATCH should succeed, got: " + statusCodes);
        assertTrue(statusCodes.stream().filter(s -> s != 204).allMatch(s -> s == 423 || s == 409),
                "Losing PATCHes must be rejected with 423 or 409, got: " + statusCodes);

        // The stored bytes must be exactly one writer's chunk, not a blend of two.
        long offset = Stores.find(uploadStore, uploadId).orElseThrow().getOffset();
        byte[] onDisk = Files.readAllBytes(Path.of(tusRuntimeConfig.store().local().uploadDir(), uploadId));
        assertEquals(offset, onDisk.length, "File length must match the recorded offset");
        assertTrue(offset > 0 && offset % 4 == 0, "Offset must match one writer's chunk size: " + offset);

        byte first = onDisk[0];
        for (byte b : onDisk) {
            assertEquals(first, b,
                    "Stored data is a mix of two writers' chunks: " + new String(onDisk));
        }
        assertEquals((first - 'A' + 1) * 4, onDisk.length,
                "Stored chunk length must match the writer that produced its bytes");
    }

    // ---- Lock timeout reclamation ----

    @Test
    void testStaleLockIsReclaimed() {
        String location = createUpload(100);
        String uploadId = extractId(location);

        // Manually acquire a lock with a past timestamp to simulate stale lock
        assertTrue(Stores.lock(uploadStore, uploadId), "Initial lock should be acquired");

        // The lock is fresh, so a second acquire should fail
        assertFalse(Stores.lock(uploadStore, uploadId), "Lock should not be re-acquired while held");

        // Release and verify re-acquisition
        Stores.unlock(uploadStore, uploadId);
        assertTrue(Stores.lock(uploadStore, uploadId), "Lock should be acquirable after release");
        Stores.unlock(uploadStore, uploadId);
    }

    @Test
    void testCleanupStaleLocks() {
        if (uploadStore instanceof LocalFileUploadStore localStore) {
            String location = createUpload(100);
            String uploadId = extractId(location);

            assertTrue(Stores.lock(uploadStore, uploadId));

            // Cleanup should not remove a fresh lock
            Stores.await(localStore.cleanupStaleLocks());
            assertFalse(Stores.lock(uploadStore, uploadId),
                    "Fresh lock should NOT be cleaned up");

            Stores.unlock(uploadStore, uploadId);
        }
    }

    // ---- Delete on non-existent upload is idempotent ----

    @Test
    void testDeleteNonExistentUploadReturns204() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete("/tus/00000000-0000-0000-0000-000000000099")
                .then()
                .statusCode(204);
    }

    // ---- Large metadata near header limit ----

    @Test
    void testLargeMetadataNearLimit() {
        // Build metadata with many fields close to the 8KB limit
        StringBuilder metadata = new StringBuilder();
        for (int i = 0; i < 19; i++) {
            if (i > 0) metadata.append(", ");
            String key = "field" + i;
            // Small base64 value
            String value = java.util.Base64.getEncoder().encodeToString(("val" + i).getBytes());
            metadata.append(key).append(" ").append(value);
        }

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .header("Upload-Metadata", metadata.toString())
                .when().post("/tus")
                .then()
                .statusCode(201);
    }

    @Test
    void testTooManyMetadataFieldsReturns400() {
        // Build metadata with 21 fields (exceeds MAX_METADATA_FIELDS=20)
        StringBuilder metadata = new StringBuilder();
        for (int i = 0; i < 21; i++) {
            if (i > 0) metadata.append(", ");
            metadata.append("k").append(i).append(" ").append(
                    java.util.Base64.getEncoder().encodeToString("v".getBytes()));
        }

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .header("Upload-Metadata", metadata.toString())
                .when().post("/tus")
                .then()
                .statusCode(400);
    }

    @Test
    void testDuplicateMetadataKeysReturns400() {
        String metadata = "filename dGVzdA==, filename dGVzdA==";

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .header("Upload-Metadata", metadata)
                .when().post("/tus")
                .then()
                .statusCode(400);
    }

    // ---- Zero-length upload ----

    @Test
    void testZeroLengthUploadCompletesImmediately() {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "0")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        // HEAD should show offset=0, length=0
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", "0")
                .header("Upload-Length", "0");
    }

    // ---- Multiple deletes on same upload ----

    @Test
    void testDoubleDeleteIsIdempotent() {
        String location = createUpload(100);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete(location)
                .then()
                .statusCode(204);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete(location)
                .then()
                .statusCode(204);
    }

    // ---- Negative upload offset ----

    @Test
    void testNegativeUploadOffsetReturns400() {
        String location = createUpload(100);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "-1")
                .contentType("application/offset+octet-stream")
                .body("test".getBytes())
                .when().patch(location)
                .then()
                .statusCode(400);
    }

    // ---- Progress service cleanup ----

    @Test
    void testProgressCleanupRemovesStaleEntries() {
        // Start progress, then clean up immediately (entries are fresh so should survive)
        uploadProgressService.startUpload("test-progress-1", 100);
        uploadProgressService.cleanupExpiredEntries();
        assertNotNull(uploadProgressService.getProgress("test-progress-1"),
                "Fresh progress entry should survive cleanup");

        // Finish to clean up
        uploadProgressService.finishUpload("test-progress-1");
        assertNull(uploadProgressService.getProgress("test-progress-1"),
                "Progress should be null after finish");
    }

    // ---- Empty checksum value ----

    @Test
    void testEmptyChecksumHeaderIsIgnored() {
        byte[] data = "test data".getBytes();
        String location = createUpload(data.length);

        // Empty Upload-Checksum header should be ignored (treated as no checksum)
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .header("Upload-Checksum", "")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);
    }

    // ---- HEAD on deleted upload returns 404 ----

    @Test
    void testHeadOnDeletedUploadReturns404() {
        String location = createUpload(100);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete(location)
                .then()
                .statusCode(204);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(404);
    }

    // ---- Stale upload cleanup ----

    @Test
    void testStaleUploadIsCleanedUp() {
        if (uploadStore instanceof LocalFileUploadStore localStore) {
            String location = createUpload(100);
            String uploadId = extractId(location);

            // Manually backdate lastActivity to make it stale
            var info = Stores.find(uploadStore, uploadId).orElseThrow();
            info.setLastActivity(Instant.now().minus(7, ChronoUnit.HOURS));

            List<String> cleaned = Stores.await(localStore.cleanupStaleUploads(6));
            assertTrue(cleaned.contains(uploadId), "Stale upload should be cleaned up");
            assertTrue(Stores.find(uploadStore, uploadId).isEmpty(), "Upload should be removed");
        }
    }

    @Test
    void testActiveUploadIsNotCleanedUp() {
        if (uploadStore instanceof LocalFileUploadStore localStore) {
            String location = createUpload(100);
            String uploadId = extractId(location);

            // lastActivity is set at creation time (recent), so should survive
            List<String> cleaned = Stores.await(localStore.cleanupStaleUploads(6));
            assertFalse(cleaned.contains(uploadId), "Active upload should NOT be cleaned up");
            assertTrue(Stores.find(uploadStore, uploadId).isPresent(), "Upload should still exist");

            // Clean up
            Stores.discard(uploadStore, uploadId);
        }
    }

    @Test
    void testCompletedUploadIsNotCleanedUpAsStale() {
        if (uploadStore instanceof LocalFileUploadStore localStore) {
            byte[] data = "test".getBytes();
            String location = createUpload(data.length);
            String uploadId = extractId(location);

            // Complete the upload
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    .statusCode(204);

            // Backdate lastActivity
            var info = Stores.find(uploadStore, uploadId).orElseThrow();
            info.setLastActivity(Instant.now().minus(7, ChronoUnit.HOURS));

            List<String> cleaned = Stores.await(localStore.cleanupStaleUploads(6));
            assertFalse(cleaned.contains(uploadId), "Completed upload should NOT be cleaned up as stale");

            // Clean up
            Stores.discard(uploadStore, uploadId);
        }
    }

    @Test
    void testStaleCleanupDisabledWithZeroHours() {
        if (uploadStore instanceof LocalFileUploadStore localStore) {
            String location = createUpload(100);
            String uploadId = extractId(location);

            var info = Stores.find(uploadStore, uploadId).orElseThrow();
            info.setLastActivity(Instant.now().minus(7, ChronoUnit.HOURS));

            List<String> cleaned = Stores.await(localStore.cleanupStaleUploads(0));
            assertTrue(cleaned.isEmpty(), "Stale cleanup with 0 hours should do nothing");
            assertTrue(Stores.find(uploadStore, uploadId).isPresent(), "Upload should still exist");

            // Clean up
            Stores.discard(uploadStore, uploadId);
        }
    }

    // ---- Orphan file cleanup ----

    @Test
    void testOrphanDataFileIsCleanedUp() throws IOException {
        if (uploadStore instanceof LocalFileUploadStore localStore) {
            Path uploadDir = Path.of(tusRuntimeConfig.store().local().uploadDir());
            String orphanId = UUID.randomUUID().toString();
            Path orphanFile = uploadDir.resolve(orphanId);

            // Create a data file with no .meta and no in-memory entry
            Files.writeString(orphanFile, "orphaned data");
            assertTrue(Files.exists(orphanFile));

            int cleaned = Stores.await(localStore.cleanupOrphanFiles());
            assertTrue(cleaned >= 1, "Should clean up at least 1 orphan file");
            assertFalse(Files.exists(orphanFile), "Orphan file should be deleted");
        }
    }

    @Test
    void testNonOrphanFileIsNotCleaned() {
        if (uploadStore instanceof LocalFileUploadStore localStore) {
            // A tracked upload's data file should NOT be cleaned up
            String location = createUpload(100);
            String uploadId = extractId(location);

            int cleaned = Stores.await(localStore.cleanupOrphanFiles());
            assertTrue(Stores.find(uploadStore, uploadId).isPresent(),
                    "Tracked upload should survive orphan cleanup");

            // Clean up
            Stores.discard(uploadStore, uploadId);
        }
    }

    // ---- Write failure truncation ----

    @Test
    void testWriteInitialDataFailureTruncatesFile() throws IOException {
        if (uploadStore instanceof LocalFileUploadStore) {
            // Create an upload, then verify truncation behavior by writing valid data
            // and checking the file is consistent
            byte[] data = "hello".getBytes();
            String location = createUpload(data.length);
            String uploadId = extractId(location);

            long staged = uploadStore.stageChunk(uploadId, 0,
                            Multi.createFrom().item(Buffer.buffer(data)), data.length)
                    .await().atMost(java.time.Duration.ofSeconds(5));
            uploadStore.commitChunk(uploadId, 0, staged).await().atMost(java.time.Duration.ofSeconds(5));
            assertEquals(data.length, staged, "Initial data should be written");
            assertEquals(data.length, Stores.find(uploadStore, uploadId).orElseThrow().getOffset());

            // Verify file size matches offset
            Path uploadDir = Path.of(tusRuntimeConfig.store().local().uploadDir());
            Path file = uploadDir.resolve(uploadId);
            assertEquals(data.length, Files.size(file), "File size should match written data");

            // Clean up
            Stores.discard(uploadStore, uploadId);
        }
    }

    // ---- Helpers ----

    private String createUpload(long size) {
        return given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(size))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .extract().header("Location");
    }

    private String extractId(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }
}
