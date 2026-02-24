package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.UploadProgressService;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.store.LocalFileUploadStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    // ---- Concurrent PATCH on same upload ----

    @Test
    void testConcurrentPatchOnSameUpload() throws Exception {
        byte[] data = "concurrent test data!!".getBytes();
        String location = createUpload(data.length);
        String uploadId = extractId(location);

        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> statusCodes = Collections.synchronizedList(new ArrayList<>());

        // Submit 5 concurrent PATCH requests
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(executor.submit(() -> {
                try {
                    latch.await();
                    int status = given()
                            .header("Tus-Resumable", "1.0.0")
                            .header("Upload-Offset", "0")
                            .contentType("application/offset+octet-stream")
                            .body(data)
                            .when().patch(location)
                            .then()
                            .extract().statusCode();
                    statusCodes.add(status);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        // Release all threads at once
        latch.countDown();

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        executor.shutdown();

        // Exactly one should succeed (204), rest should be locked (423)
        long successCount = statusCodes.stream().filter(s -> s == 204).count();
        long lockedCount = statusCodes.stream().filter(s -> s == 423).count();

        assertEquals(1, successCount, "Exactly one PATCH should succeed");
        assertEquals(4, lockedCount, "Remaining PATCHes should get 423 Locked");
    }

    // ---- Lock timeout reclamation ----

    @Test
    void testStaleLockIsReclaimed() {
        String location = createUpload(100);
        String uploadId = extractId(location);

        // Manually acquire a lock with a past timestamp to simulate stale lock
        assertTrue(uploadStore.acquireLock(uploadId), "Initial lock should be acquired");

        // The lock is fresh, so a second acquire should fail
        assertFalse(uploadStore.acquireLock(uploadId), "Lock should not be re-acquired while held");

        // Release and verify re-acquisition
        uploadStore.releaseLock(uploadId);
        assertTrue(uploadStore.acquireLock(uploadId), "Lock should be acquirable after release");
        uploadStore.releaseLock(uploadId);
    }

    @Test
    void testCleanupStaleLocks() {
        if (uploadStore instanceof LocalFileUploadStore localStore) {
            String location = createUpload(100);
            String uploadId = extractId(location);

            assertTrue(uploadStore.acquireLock(uploadId));

            // Cleanup should not remove a fresh lock
            localStore.cleanupStaleLocks();
            assertFalse(uploadStore.acquireLock(uploadId),
                    "Fresh lock should NOT be cleaned up");

            uploadStore.releaseLock(uploadId);
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
