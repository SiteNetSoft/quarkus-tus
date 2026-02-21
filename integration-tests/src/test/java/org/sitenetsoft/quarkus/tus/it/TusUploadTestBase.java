package org.sitenetsoft.quarkus.tus.it;

import io.restassured.response.Response;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

/**
 * HTTP-only TUS protocol tests (no CDI injection required).
 * Extended by both {@code @QuarkusTest} and {@code @QuarkusIntegrationTest} subclasses.
 */
abstract class TusUploadTestBase {

    // ---- Helpers ----

    protected String createUpload(long size) {
        return given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(size))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .extract().header("Location");
    }

    protected String createPartialUpload(long size) {
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

    protected void uploadData(String location, byte[] data, long offset) {
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

    protected String extractId(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }

    // ---- Tests ----

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

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204)
                .header("Upload-Offset", String.valueOf(data.length));

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", String.valueOf(data.length))
                .header("Upload-Length", String.valueOf(data.length));
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

    @Test
    void testMultiChunkOffsetMismatchReturns409() {
        byte[] data = "test data".getBytes();
        String location = createUpload(100);

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

        uploadData(location, firstHalf, 0);

        String offset = given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .extract().header("Upload-Offset");

        assertEquals(String.valueOf(firstHalf.length), offset);

        uploadData(location, secondHalf, Long.parseLong(offset));

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", String.valueOf(fullData.length))
                .header("Upload-Length", String.valueOf(fullData.length));
    }

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
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().post("/tus")
                .then()
                .statusCode(400);
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

        assertNull(response.header("Upload-Offset"),
                "Upload-Offset should be absent for deferred with body");

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", "0")
                .header("Upload-Defer-Length", "1");
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

        assertDoesNotThrow(() ->
                ZonedDateTime.parse(expiresHeader, DateTimeFormatter.RFC_1123_DATE_TIME));
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

    @Test
    void testConcatenationFailsWithMissingPartials() {
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
}
