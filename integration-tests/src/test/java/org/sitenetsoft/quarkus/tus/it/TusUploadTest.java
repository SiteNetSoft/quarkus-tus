package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.UploadExpirationScheduler;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.time.Instant;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class TusUploadTest extends TusUploadTestBase {

    @Inject
    TusTestObserver observer;

    @Inject
    UploadStore uploadStore;

    @Inject
    UploadExpirationScheduler expirationScheduler;

    @BeforeEach
    void setUp() {
        observer.reset();
    }

    // ---- Injection-dependent tests ----

    @Test
    void testPostCreate() {
        String location = createUpload(100);
        assertNotNull(location);

        assertFalse(observer.createdEvents.isEmpty(), "Expected TusUploadCreatedEvent to be fired");
        assertEquals(100, observer.createdEvents.getFirst().totalSize());
    }

    @Test
    void testDelete() {
        String location = createUpload(100);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete(location)
                .then()
                .statusCode(204);

        assertFalse(observer.terminatedEvents.isEmpty(), "Expected TusUploadTerminatedEvent to be fired");

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(404);
    }

    @Test
    void testMultiChunkUploadWithEvents() {
        byte[] chunk1 = "chunk1".getBytes();
        byte[] chunk2 = "chunk2".getBytes();
        byte[] chunk3 = "chunk3".getBytes();
        long totalSize = chunk1.length + chunk2.length + chunk3.length;

        String location = createUpload(totalSize);

        uploadData(location, chunk1, 0);
        uploadData(location, chunk2, chunk1.length);
        uploadData(location, chunk3, chunk1.length + chunk2.length);

        assertEquals(3, observer.chunkEvents.size(), "Expected 3 TusChunkReceivedEvents");
        assertFalse(observer.completedEvents.isEmpty(), "Expected TusUploadCompletedEvent on final chunk");
        assertEquals(1, observer.completedEvents.size(), "Only one completed event should fire");
    }

    @Test
    void testMultiChunkNoCompletedEventUntilDone() {
        byte[] chunk1 = "first".getBytes();
        byte[] chunk2 = "second".getBytes();
        long totalSize = chunk1.length + chunk2.length;

        String location = createUpload(totalSize);

        uploadData(location, chunk1, 0);

        assertTrue(observer.completedEvents.isEmpty(),
                "No completed event should fire for non-final chunk");

        uploadData(location, chunk2, chunk1.length);

        assertFalse(observer.completedEvents.isEmpty(),
                "Completed event should fire for final chunk");
    }

    @Test
    void testCreationWithUploadComplete() {
        byte[] data = "creation with upload".getBytes();

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .header("Upload-Offset", String.valueOf(data.length));

        assertFalse(observer.completedEvents.isEmpty(),
                "Expected TusUploadCompletedEvent for creation-with-upload");
    }

    @Test
    void testCreationWithUploadPartialBody() {
        byte[] fullData = "hello world 12345".getBytes();
        byte[] partialData = "hello".getBytes();

        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(fullData.length))
                .contentType("application/offset+octet-stream")
                .body(partialData)
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Upload-Offset", String.valueOf(partialData.length))
                .extract().header("Location");

        assertTrue(observer.completedEvents.isEmpty(),
                "Upload not complete yet after partial creation-with-upload");

        byte[] remaining = java.util.Arrays.copyOfRange(fullData, partialData.length, fullData.length);
        uploadData(location, remaining, partialData.length);

        assertFalse(observer.completedEvents.isEmpty(),
                "Expected TusUploadCompletedEvent after completing upload");
    }

    @Test
    void testHeadOnExpiredUploadReturns410() {
        String location = createUpload(100);
        String uploadId = extractId(location);

        uploadStore.findUploadInfo(uploadId).ifPresent(
                info -> info.setExpiresAt(Instant.now().minusSeconds(3600)));

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(410);
    }

    @Test
    void testPatchOnExpiredUploadReturns410() {
        byte[] data = "expired data".getBytes();
        String location = createUpload(data.length);
        String uploadId = extractId(location);

        uploadStore.findUploadInfo(uploadId).ifPresent(
                info -> info.setExpiresAt(Instant.now().minusSeconds(3600)));

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(410);
    }

    @Test
    void testCleanupExpiredUploads() {
        String loc1 = createUpload(100);
        String loc2 = createUpload(100);
        String loc3 = createUpload(100);

        String id1 = extractId(loc1);
        String id2 = extractId(loc2);
        String id3 = extractId(loc3);

        uploadStore.findUploadInfo(id1).ifPresent(
                info -> info.setExpiresAt(Instant.now().minusSeconds(3600)));
        uploadStore.findUploadInfo(id2).ifPresent(
                info -> info.setExpiresAt(Instant.now().minusSeconds(3600)));

        List<String> cleaned = uploadStore.cleanupExpiredUploads();
        assertEquals(2, cleaned.size(), "Should clean up exactly 2 expired uploads");

        assertTrue(uploadStore.findUploadInfo(id3).isPresent(),
                "Non-expired upload should still exist");
        assertTrue(uploadStore.findUploadInfo(id1).isEmpty(),
                "Expired upload 1 should be removed");
        assertTrue(uploadStore.findUploadInfo(id2).isEmpty(),
                "Expired upload 2 should be removed");
    }

    @Test
    void testConcatenationPartialAndFinalMerge() {
        byte[] data1 = "part one".getBytes();
        byte[] data2 = " part two".getBytes();

        String loc1 = createPartialUpload(data1.length);
        String loc2 = createPartialUpload(data2.length);

        uploadData(loc1, data1, 0);
        uploadData(loc2, data2, 0);

        String finalLocation = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Concat", "final; " + loc1 + " " + loc2)
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .extract().header("Location");

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(finalLocation)
                .then()
                .statusCode(200)
                .header("Upload-Offset", String.valueOf(data1.length + data2.length))
                .header("Upload-Length", String.valueOf(data1.length + data2.length));

        assertFalse(observer.concatEvents.isEmpty(),
                "Expected TusConcatenationCompletedEvent");

        String id1 = extractId(loc1);
        String id2 = extractId(loc2);
        assertTrue(uploadStore.findUploadInfo(id1).isEmpty(),
                "Partial 1 should be removed after merge");
        assertTrue(uploadStore.findUploadInfo(id2).isEmpty(),
                "Partial 2 should be removed after merge");
    }

    @Test
    void testPatchOnLockedUploadReturns423() {
        String location = createUpload(100);
        String uploadId = extractId(location);

        assertTrue(uploadStore.acquireLock(uploadId), "Lock should be acquired");

        try {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body("locked".getBytes())
                    .when().patch(location)
                    .then()
                    .statusCode(423);
        } finally {
            uploadStore.releaseLock(uploadId);
        }
    }

    @Test
    void testConcurrentPatchReturns423ForSecondWriter() {
        byte[] data = "concurrent test".getBytes();
        String location = createUpload(data.length);
        String uploadId = extractId(location);

        assertTrue(uploadStore.acquireLock(uploadId), "Lock should be acquired");

        try {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    .statusCode(423);
        } finally {
            uploadStore.releaseLock(uploadId);
        }

        // After releasing, a normal PATCH should succeed
        uploadData(location, data, 0);
    }

    @Test
    void testExpirationSchedulerDelegates() {
        String loc1 = createUpload(100);
        String loc2 = createUpload(100);
        String id1 = extractId(loc1);
        String id2 = extractId(loc2);

        uploadStore.findUploadInfo(id1).ifPresent(
                info -> info.setExpiresAt(Instant.now().minusSeconds(3600)));

        expirationScheduler.cleanupExpiredUploads();

        assertTrue(uploadStore.findUploadInfo(id1).isEmpty(),
                "Expired upload should be removed by scheduler");
        assertTrue(uploadStore.findUploadInfo(id2).isPresent(),
                "Non-expired upload should still exist");
    }
}
