package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.Response;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.UploadExpirationScheduler;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class TusUploadTest {

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

    private String createPartialUpload(long size) {
        return given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(size))
                .header("Upload-Concat", "partial")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .extract().header("Location");
    }

    private void uploadData(String location, byte[] data, long offset) {
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", String.valueOf(offset))
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204)
                .header("Upload-Offset", String.valueOf(offset + data.length));
    }

    private String extractId(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }

    // ---- Existing tests ----

    @Test
    void testOptions() {
        given()
                .when().options("/tus")
                .then()
                .statusCode(204)
                .header("Tus-Resumable", notNullValue())
                .header("Tus-Version", notNullValue())
                .header("Tus-Max-Size", notNullValue())
                .header("Tus-Extension", notNullValue())
                .header("Tus-Checksum-Algorithm", notNullValue());
    }

    @Test
    void testPostCreate() {
        String location = createUpload(100);
        assertNotNull(location);

        assertFalse(observer.createdEvents.isEmpty(), "Expected TusUploadCreatedEvent to be fired");
        assertEquals(100, observer.createdEvents.getFirst().totalSize());
    }

    @Test
    void testHeadStatus() {
        String location = createUpload(100);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", "0")
                .header("Upload-Length", "100");
    }

    @Test
    void testPatchChunk() {
        byte[] data = "hello world".getBytes();
        String location = createUpload(data.length);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204)
                .header("Upload-Offset", String.valueOf(data.length));
    }

    @Test
    void testFullUploadCycle() {
        byte[] data = "complete upload data".getBytes();
        String location = createUpload(data.length);

        // Patch
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204)
                .header("Upload-Offset", String.valueOf(data.length));

        // Head should show complete
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", String.valueOf(data.length))
                .header("Upload-Length", String.valueOf(data.length));
    }

    @Test
    void testDelete() {
        String location = createUpload(100);

        // Delete
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete(location)
                .then()
                .statusCode(204);

        assertFalse(observer.terminatedEvents.isEmpty(), "Expected TusUploadTerminatedEvent to be fired");

        // Head should be not found
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(404);
    }

    @Test
    void testValidChecksum() throws Exception {
        byte[] data = "checksum test".getBytes();
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        String checksum = "sha1 " + Base64.getEncoder().encodeToString(md.digest(data));

        String location = createUpload(data.length);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .header("Upload-Checksum", checksum)
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);
    }

    @Test
    void testInvalidChecksum() {
        byte[] data = "checksum test".getBytes();
        String location = createUpload(data.length);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .header("Upload-Checksum", "sha1 AAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(460);
    }

    @Test
    void testInvalidUploadId() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head("/tus/not-a-uuid")
                .then()
                .statusCode(400);
    }

    @Test
    void testMissingTusResumableHeader() {
        given()
                .when().head("/tus/550e8400-e29b-41d4-a716-446655440000")
                .then()
                .statusCode(412);
    }

    // ---- Multi-Chunk tests ----

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
    void testMultiChunkOffsetMismatchReturns409() {
        byte[] data = "test data".getBytes();
        String location = createUpload(100);

        // Send with wrong offset (server expects 0, client sends 5)
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "5")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(409)
                .header("Upload-Offset", "0");
    }

    @Test
    void testChunkExceedingRemainingLengthReturns409() {
        String location = createUpload(5);

        byte[] data = "this is way too long for 5 bytes".getBytes();

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(409);
    }

    @Test
    void testMultiChunkResumeAfterInterruption() {
        byte[] fullData = "resumable upload test data!!".getBytes();
        byte[] firstHalf = Arrays.copyOfRange(fullData, 0, 14);
        byte[] secondHalf = Arrays.copyOfRange(fullData, 14, fullData.length);

        String location = createUpload(fullData.length);

        // Upload first half
        uploadData(location, firstHalf, 0);

        // HEAD to discover offset (simulating client reconnect)
        String offset = given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .extract().header("Upload-Offset");

        assertEquals(String.valueOf(firstHalf.length), offset);

        // Resume from discovered offset
        uploadData(location, secondHalf, Long.parseLong(offset));

        // Verify complete
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", String.valueOf(fullData.length))
                .header("Upload-Length", String.valueOf(fullData.length));
    }

    @Test
    void testMultiChunkNoCompletedEventUntilDone() {
        byte[] chunk1 = "first".getBytes();
        byte[] chunk2 = "second".getBytes();
        long totalSize = chunk1.length + chunk2.length;

        String location = createUpload(totalSize);

        // Upload first chunk (non-final)
        uploadData(location, chunk1, 0);

        // Completed event should NOT have fired yet
        assertTrue(observer.completedEvents.isEmpty(),
                "No completed event should fire for non-final chunk");

        // Upload second chunk (final)
        uploadData(location, chunk2, chunk1.length);

        // Now completed event should have fired
        assertFalse(observer.completedEvents.isEmpty(),
                "Completed event should fire for final chunk");
    }

    // ---- Deferred Length tests ----

    @Test
    void testDeferredLengthCreation() {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Defer-Length", "1")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .header("Upload-Defer-Length", "1")
                .extract().header("Location");

        // HEAD should show deferred length, no Upload-Length
        Response headResponse = given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location);

        headResponse.then()
                .statusCode(200)
                .header("Upload-Defer-Length", "1")
                .header("Upload-Offset", "0");

        assertNull(headResponse.header("Upload-Length"),
                "Upload-Length should be absent for deferred upload");
    }

    @Test
    void testDeferredLengthSetViaFirstPatch() {
        byte[] data = "deferred data!".getBytes();

        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Defer-Length", "1")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        // PATCH with Upload-Length to set the deferred length, then upload data
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .header("Upload-Length", String.valueOf(data.length))
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204)
                .header("Upload-Offset", String.valueOf(data.length));

        // HEAD should now show the set length
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Length", String.valueOf(data.length))
                .header("Upload-Offset", String.valueOf(data.length));
    }

    @Test
    void testDeferredLengthPatchWithoutLengthReturns400() {
        byte[] data = "some data".getBytes();

        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Defer-Length", "1")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        // PATCH without Upload-Length when deferred -> 400
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(400);
    }

    @Test
    void testCreationRequiresLengthOrDeferred() {
        // POST without Upload-Length and without Upload-Defer-Length -> 400
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().post("/tus")
                .then()
                .statusCode(400);
    }

    // ---- Creation-with-Upload tests ----

    @Test
    void testCreationWithUploadComplete() {
        byte[] data = "creation with upload".getBytes();

        Response response = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().post("/tus");

        response.then()
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

        Response createResponse = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(fullData.length))
                .contentType("application/offset+octet-stream")
                .body(partialData)
                .when().post("/tus");

        String location = createResponse.then()
                .statusCode(201)
                .header("Upload-Offset", String.valueOf(partialData.length))
                .extract().header("Location");

        // Should not be completed yet
        assertTrue(observer.completedEvents.isEmpty(),
                "Upload not complete yet after partial creation-with-upload");

        // Complete with PATCH
        byte[] remaining = Arrays.copyOfRange(fullData, partialData.length, fullData.length);
        uploadData(location, remaining, partialData.length);

        assertFalse(observer.completedEvents.isEmpty(),
                "Expected TusUploadCompletedEvent after completing upload");
    }

    @Test
    void testCreationWithUploadWrongContentType() {
        byte[] data = "should fail".getBytes();

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .contentType("text/plain")
                .body(data)
                .when().post("/tus")
                .then()
                .statusCode(400);
    }

    @Test
    void testCreationWithUploadOnDeferredIgnoresBody() {
        byte[] data = "should be ignored".getBytes();

        Response response = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Defer-Length", "1")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().post("/tus");

        String location = response.then()
                .statusCode(201)
                .extract().header("Location");

        // Upload-Offset should not be in the response since body was ignored
        assertNull(response.header("Upload-Offset"),
                "Upload-Offset should be absent for deferred with body");

        // HEAD should show offset 0 and deferred
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", "0")
                .header("Upload-Defer-Length", "1");
    }

    // ---- Expiration tests ----

    @Test
    void testHeadOnExpiredUploadReturns410() {
        String location = createUpload(100);
        String uploadId = extractId(location);

        // Set expiration to the past
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

        // Set expiration to the past
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
    void testHeadReturnsUploadExpiresHeader() {
        String location = createUpload(100);

        String expiresHeader = given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Expires", notNullValue())
                .extract().header("Upload-Expires");

        // Verify RFC 1123 format
        assertDoesNotThrow(() ->
                ZonedDateTime.parse(expiresHeader, DateTimeFormatter.RFC_1123_DATE_TIME));
    }

    @Test
    void testCleanupExpiredUploads() {
        String loc1 = createUpload(100);
        String loc2 = createUpload(100);
        String loc3 = createUpload(100);

        String id1 = extractId(loc1);
        String id2 = extractId(loc2);
        String id3 = extractId(loc3);

        // Expire 2 of them
        uploadStore.findUploadInfo(id1).ifPresent(
                info -> info.setExpiresAt(Instant.now().minusSeconds(3600)));
        uploadStore.findUploadInfo(id2).ifPresent(
                info -> info.setExpiresAt(Instant.now().minusSeconds(3600)));

        List<String> cleaned = uploadStore.cleanupExpiredUploads();
        assertEquals(2, cleaned.size(), "Should clean up exactly 2 expired uploads");

        // id3 should still exist
        assertTrue(uploadStore.findUploadInfo(id3).isPresent(),
                "Non-expired upload should still exist");
        // id1 and id2 should be gone
        assertTrue(uploadStore.findUploadInfo(id1).isEmpty(),
                "Expired upload 1 should be removed");
        assertTrue(uploadStore.findUploadInfo(id2).isEmpty(),
                "Expired upload 2 should be removed");
    }

    @Test
    void testPostCreateIncludesUploadExpiresHeader() {
        Response response = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .when().post("/tus");

        String expiresHeader = response.then()
                .statusCode(201)
                .header("Upload-Expires", notNullValue())
                .extract().header("Upload-Expires");

        assertDoesNotThrow(() ->
                ZonedDateTime.parse(expiresHeader, DateTimeFormatter.RFC_1123_DATE_TIME));
    }

    // ---- Concatenation tests ----

    @Test
    void testConcatenationPartialAndFinalMerge() {
        byte[] data1 = "part one".getBytes();
        byte[] data2 = " part two".getBytes();

        // Create partial uploads
        String loc1 = createPartialUpload(data1.length);
        String loc2 = createPartialUpload(data2.length);

        // Upload data to each partial
        uploadData(loc1, data1, 0);
        uploadData(loc2, data2, 0);

        // Create final concat
        String finalLocation = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Concat", "final; " + loc1 + " " + loc2)
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .extract().header("Location");

        // Verify merged upload via HEAD
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(finalLocation)
                .then()
                .statusCode(200)
                .header("Upload-Offset", String.valueOf(data1.length + data2.length))
                .header("Upload-Length", String.valueOf(data1.length + data2.length));

        // Verify concatenation event fired
        assertFalse(observer.concatEvents.isEmpty(),
                "Expected TusConcatenationCompletedEvent");

        // Verify partials are cleaned up
        String id1 = extractId(loc1);
        String id2 = extractId(loc2);
        assertTrue(uploadStore.findUploadInfo(id1).isEmpty(),
                "Partial 1 should be removed after merge");
        assertTrue(uploadStore.findUploadInfo(id2).isEmpty(),
                "Partial 2 should be removed after merge");
    }

    @Test
    void testConcatenationFailsWithMissingPartials() {
        // Reference non-existent partial upload IDs
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Concat",
                        "final; /tus/00000000-0000-0000-0000-000000000001 /tus/00000000-0000-0000-0000-000000000002")
                .when().post("/tus")
                .then()
                .statusCode(400);
    }

    @Test
    void testConcatenationWithNoPartials() {
        // "final;" with no IDs -> 400
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Concat", "final;")
                .when().post("/tus")
                .then()
                .statusCode(400);
    }

    @Test
    void testHeadOnPartialUpload() {
        String location = createPartialUpload(50);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Concat", "partial")
                .header("Upload-Length", "50")
                .header("Upload-Offset", "0");
    }

    // ---- Protocol enforcement tests ----

    @Test
    void testPostWithoutTusResumableReturns412() {
        given()
                .header("Upload-Length", "100")
                .when().post("/tus")
                .then()
                .statusCode(412);
    }

    @Test
    void testDeleteWithoutTusResumableReturns412() {
        given()
                .when().delete("/tus/550e8400-e29b-41d4-a716-446655440000")
                .then()
                .statusCode(412);
    }

    @Test
    void testPostExceedingMaxSizeReturns413() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "104857601")
                .when().post("/tus")
                .then()
                .statusCode(413);
    }

    @Test
    void testPatchExceedingMaxChunkSizeReturns413() {
        byte[] largeChunk = new byte[1025];
        String location = createUpload(10000);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(largeChunk)
                .when().patch(location)
                .then()
                .statusCode(413);
    }

    // ---- PATCH edge case tests ----

    @Test
    void testPatchNonExistentUploadReturns404() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body("test".getBytes())
                .when().patch("/tus/00000000-0000-0000-0000-000000000099")
                .then()
                .statusCode(404);
    }

    @Test
    void testPatchOnLockedUploadReturns423() {
        String location = createUpload(100);
        String uploadId = extractId(location);

        // Pre-acquire lock to simulate concurrent upload
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
    void testUnsupportedChecksumAlgorithmReturns460() {
        byte[] data = "checksum algo test".getBytes();
        String location = createUpload(data.length);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .header("Upload-Checksum", "blake2b AAAAAAAAAAAAAAAAAAAAAA==")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(460);
    }

    @Test
    void testCreationWithUploadExceedingMaxChunkSizeReturns413() {
        // max-chunk-size is 1024 in test config
        byte[] largeBody = new byte[1025];

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "10000")
                .contentType("application/offset+octet-stream")
                .body(largeBody)
                .when().post("/tus")
                .then()
                .statusCode(413);
    }

    // ---- Checksum algorithm tests ----

    @Test
    void testMd5Checksum() throws Exception {
        byte[] data = "md5 checksum test".getBytes();
        MessageDigest md = MessageDigest.getInstance("MD5");
        String checksum = "md5 " + Base64.getEncoder().encodeToString(md.digest(data));

        String location = createUpload(data.length);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .header("Upload-Checksum", checksum)
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);
    }

    @Test
    void testSha256Checksum() throws Exception {
        byte[] data = "sha256 checksum test".getBytes();
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        String checksum = "sha256 " + Base64.getEncoder().encodeToString(md.digest(data));

        String location = createUpload(data.length);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .header("Upload-Checksum", checksum)
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);
    }

    // ---- Scheduler tests ----

    @Test
    void testExpirationSchedulerDelegates() {
        String loc1 = createUpload(100);
        String loc2 = createUpload(100);
        String id1 = extractId(loc1);
        String id2 = extractId(loc2);

        // Expire one of them
        uploadStore.findUploadInfo(id1).ifPresent(
                info -> info.setExpiresAt(Instant.now().minusSeconds(3600)));

        // Invoke the scheduler's method directly
        expirationScheduler.cleanupExpiredUploads();

        // Verify delegation worked
        assertTrue(uploadStore.findUploadInfo(id1).isEmpty(),
                "Expired upload should be removed by scheduler");
        assertTrue(uploadStore.findUploadInfo(id2).isPresent(),
                "Non-expired upload should still exist");
    }
}
