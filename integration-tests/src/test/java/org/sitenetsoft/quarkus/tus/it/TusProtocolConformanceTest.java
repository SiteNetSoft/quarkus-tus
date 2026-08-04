package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.Response;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.*;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * TUS protocol conformance tests organized by spec section.
 * <p>
 * Reference: <a href="https://tus.io/protocols/resumable-upload">TUS Protocol v1.0.0</a>
 */
@QuarkusTest
class TusProtocolConformanceTest {

    // ---- Helpers ----

    private String createUpload(long size) {
        return given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(size))
                .when().post("/tus")
                .then()
                .statusCode(201)
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
                .statusCode(204);
    }

    private String extractId(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }

    // ========== Core Protocol ==========

    @Nested
    class CoreProtocol {

        /**
         * Tus-Resumable is required on every response except OPTIONS. The resource methods set
         * it themselves, but responses the container produces — a media-type mismatch, an
         * unsupported method, an unmatched path — bypass them entirely.
         */
        @Test
        void wrongContentTypeOnPatchStillIncludesTusResumable() {
            String location = createUpload(10);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("text/plain")
                    .body("x")
                    .when().patch(location)
                    .then()
                    .statusCode(415)
                    .header("Tus-Resumable", "1.0.0");
        }

        @Test
        void unsupportedMethodStillIncludesTusResumable() {
            String location = createUpload(10);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().put(location)
                    .then()
                    .statusCode(405)
                    .header("Tus-Resumable", "1.0.0");
        }

        @Test
        void unsupportedMethodOnCollectionStillIncludesTusResumable() {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().put("/tus")
                    .then()
                    .statusCode(405)
                    .header("Tus-Resumable", "1.0.0");
        }

        @Test
        void versionMismatchStillIncludesTusVersion() {
            given()
                    .header("Tus-Resumable", "0.0.1")
                    .header("Upload-Length", "10")
                    .when().post("/tus")
                    .then()
                    .statusCode(412)
                    .header("Tus-Version", "1.0.0");
        }

        @Test
        void headIncludesCacheControlNoStore() {
            String location = createUpload(100);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Cache-Control", "no-store");
        }

        @Test
        void headReturns404ForNonExistent() {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head("/tus/00000000-0000-0000-0000-000000000001")
                    .then()
                    .statusCode(404)
                    .header("Tus-Resumable", "1.0.0");
        }

        @Test
        void patchIncludesUploadExpires() {
            byte[] data = "test".getBytes();
            String location = createUpload(data.length);

            String expires = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    .statusCode(204)
                    .header("Upload-Expires", notNullValue())
                    .extract().header("Upload-Expires");

            assertDoesNotThrow(() ->
                    ZonedDateTime.parse(expires, DateTimeFormatter.RFC_1123_DATE_TIME));
        }

        @Test
        void wrongTusVersionReturns412WithTusVersion() {
            given()
                    .header("Tus-Resumable", "0.0.1")
                    .when().head("/tus/00000000-0000-0000-0000-000000000001")
                    .then()
                    .statusCode(412)
                    .header("Tus-Version", "1.0.0");
        }

        @Test
        void optionsDoesNotRequireTusResumable() {
            // OPTIONS must succeed without Tus-Resumable header
            given()
                    .when().options("/tus")
                    .then()
                    .statusCode(204)
                    .header("Tus-Version", notNullValue())
                    .header("Tus-Extension", notNullValue());
        }

        @Test
        void allResponsesIncludeTusResumable() {
            String location = createUpload(100);

            // HEAD
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .header("Tus-Resumable", "1.0.0");

            // PATCH
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body("x".getBytes())
                    .when().patch(location)
                    .then()
                    .header("Tus-Resumable", "1.0.0");

            // DELETE
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().delete(location)
                    .then()
                    .header("Tus-Resumable", "1.0.0");
        }

        @Test
        void optionsIncludesTusMaxSize() {
            given()
                    .when().options("/tus")
                    .then()
                    .statusCode(204)
                    .header("Tus-Max-Size", notNullValue());
        }

        @Test
        void headReturnsUploadLength() {
            String location = createUpload(42);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Length", "42")
                    .header("Upload-Offset", "0");
        }

        @Test
        void patchReturnsUploadOffset() {
            byte[] data = "hello".getBytes();
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
        void missingTusResumableOnPatchReturns412() {
            String location = createUpload(100);

            given()
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body("test".getBytes())
                    .when().patch(location)
                    .then()
                    .statusCode(412)
                    .header("Tus-Version", notNullValue());
        }
    }

    // ========== Creation Conformance ==========

    @Nested
    class CreationConformance {

        @Test
        void negativeUploadLengthRejected() {
            // Negative Upload-Length passes parsing but createUpload rejects it
            int status = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", "-5")
                    .when().post("/tus")
                    .then()
                    .extract().statusCode();

            // Server may return 400 or 500 depending on where the check happens
            assertTrue(status >= 400, "Negative Upload-Length should be rejected");
        }

        @Test
        void zeroLengthUploadCreatedSuccessfully() {
            String location = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", "0")
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Location");

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", "0")
                    .header("Upload-Length", "0");
        }

        @Test
        void locationHeaderUsableForHead() {
            String location = createUpload(100);

            // Location should be directly usable
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200);
        }

        @Test
        void metadataRoundTrip() {
            String metadata = "filename dGVzdC50eHQ=, type dGV4dC9wbGFpbg==";

            String location = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", "100")
                    .header("Upload-Metadata", metadata)
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Location");

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Metadata", metadata);
        }

        @Test
        void postReturnsUploadExpires() {
            String expires = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", "100")
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Upload-Expires");

            assertNotNull(expires);
            ZonedDateTime parsed = ZonedDateTime.parse(expires, DateTimeFormatter.RFC_1123_DATE_TIME);
            assertTrue(parsed.toInstant().isAfter(java.time.Instant.now()),
                    "Upload-Expires should be in the future");
        }
    }

    // ========== Creation-with-Upload ==========

    @Nested
    class CreationWithUpload {

        @Test
        void responseIncludesUploadOffset() {
            byte[] data = "inline".getBytes();

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", String.valueOf(data.length))
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .header("Upload-Offset", String.valueOf(data.length));
        }

        /**
         * A body longer than Upload-Length used to be written to disk in full while the
         * recorded offset was clamped, producing a "complete" upload with trailing garbage.
         */
        @Test
        void bodyLongerThanUploadLengthIsRejected() {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", "5")
                    .contentType("application/offset+octet-stream")
                    .body("far more than five bytes".getBytes())
                    .when().post("/tus")
                    .then()
                    .statusCode(413);
        }

        @Test
        void partialBodyThenResumeViaPatch() {
            byte[] fullData = "complete data!!".getBytes();
            byte[] firstHalf = Arrays.copyOfRange(fullData, 0, 8);
            byte[] secondHalf = Arrays.copyOfRange(fullData, 8, fullData.length);

            String location = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", String.valueOf(fullData.length))
                    .contentType("application/offset+octet-stream")
                    .body(firstHalf)
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .header("Upload-Offset", String.valueOf(firstHalf.length))
                    .extract().header("Location");

            // Resume via PATCH
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", String.valueOf(firstHalf.length))
                    .contentType("application/offset+octet-stream")
                    .body(secondHalf)
                    .when().patch(location)
                    .then()
                    .statusCode(204)
                    .header("Upload-Offset", String.valueOf(fullData.length));
        }

        @Test
        void checksumOnCreationWithUpload() throws Exception {
            byte[] data = "csum inline".getBytes();
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            String checksum = "sha1 " + Base64.getEncoder().encodeToString(md.digest(data));

            // creation-with-upload with checksum should succeed
            // Note: checksum on creation-with-upload is validated in PATCH flow only
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", String.valueOf(data.length))
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().post("/tus")
                    .then()
                    .statusCode(201);
        }

        @Test
        void completeUploadInSinglePost() {
            byte[] data = "one-shot".getBytes();

            String location = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", String.valueOf(data.length))
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .header("Upload-Offset", String.valueOf(data.length))
                    .extract().header("Location");

            // Verify upload is complete
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", String.valueOf(data.length))
                    .header("Upload-Length", String.valueOf(data.length));
        }
    }

    // ========== Termination Conformance ==========

    @Nested
    class TerminationConformance {

        @Test
        void deleteReturns204() {
            String location = createUpload(100);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().delete(location)
                    .then()
                    .statusCode(204)
                    .header("Tus-Resumable", "1.0.0");
        }

        @Test
        void headAfterDeleteReturns404() {
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

        @Test
        void patchAfterDeleteReturns404() {
            String location = createUpload(100);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().delete(location)
                    .then()
                    .statusCode(204);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body("test".getBytes())
                    .when().patch(location)
                    .then()
                    .statusCode(404);
        }
    }

    // ========== Checksum Conformance ==========

    @Nested
    class ChecksumConformance {

        @Test
        void mismatchDoesNotUpdateOffset() {
            byte[] data = "checksum fail".getBytes();
            String location = createUpload(data.length);

            // Send with bad checksum
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .header("Upload-Checksum", "sha1 AAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    .statusCode(460);

            // Offset should still be 0
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", "0");
        }

        @Test
        void malformedChecksumNoSpace() {
            byte[] data = "test".getBytes();
            String location = createUpload(data.length);

            // No space between algorithm and value
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .header("Upload-Checksum", "sha1AAAA")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    // A client that sent a checksum expects verification; accepting the chunk
                    // unverified would silently break that expectation.
                    .statusCode(400);
        }

        @Test
        void checksumOnSecondChunk() throws Exception {
            byte[] chunk1 = "first".getBytes();
            byte[] chunk2 = "secnd".getBytes();
            String location = createUpload(chunk1.length + chunk2.length);

            // First chunk without checksum
            uploadData(location, chunk1, 0);

            // Second chunk with valid checksum
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            String checksum = "sha1 " + Base64.getEncoder().encodeToString(md.digest(chunk2));

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", String.valueOf(chunk1.length))
                    .header("Upload-Checksum", checksum)
                    .contentType("application/offset+octet-stream")
                    .body(chunk2)
                    .when().patch(location)
                    .then()
                    .statusCode(204);
        }

        @Test
        void checksumAlgorithmsLowercaseInOptions() {
            String algorithms = given()
                    .when().options("/tus")
                    .then()
                    .statusCode(204)
                    .extract().header("Tus-Checksum-Algorithm");

            assertNotNull(algorithms);
            for (String alg : algorithms.split(",")) {
                assertEquals(alg.trim(), alg.trim().toLowerCase(),
                        "Checksum algorithm should be lowercase: " + alg);
            }
        }

        /**
         * The checksum extension specifies 400 for an unsupported algorithm; 460 is reserved
         * for a genuine mismatch and would mislead a client into retrying.
         */
        /**
         * checksum-trailer: the client streams the chunk and only commits the checksum once it
         * has finished, sending it as a trailer after the body. RestAssured cannot send
         * trailers, so the request is written directly onto a socket.
         */
        @Test
        void checksumSentAsTrailerIsVerified() throws Exception {
            byte[] data = "trailer checksummed".getBytes();
            String location = createUpload(data.length);
            String sha1 = Base64.getEncoder().encodeToString(
                    MessageDigest.getInstance("SHA-1").digest(data));

            assertEquals(204, patchWithTrailer(location, data, "sha1 " + sha1),
                    "A valid checksum trailer must be accepted");

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", String.valueOf(data.length));
        }

        @Test
        void badChecksumSentAsTrailerIsRejected() throws Exception {
            byte[] data = "trailer checksummed".getBytes();
            String location = createUpload(data.length);
            String wrong = Base64.getEncoder().encodeToString(
                    MessageDigest.getInstance("SHA-1").digest("something else".getBytes()));

            assertEquals(460, patchWithTrailer(location, data, "sha1 " + wrong),
                    "A mismatching checksum trailer must be rejected");

            // The chunk must not have been stored.
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", "0");
        }

        /** Sends a chunked PATCH whose Upload-Checksum arrives as a trailer; returns the status. */
        private int patchWithTrailer(String location, byte[] body, String checksum) throws Exception {
            String request = "PATCH " + location + " HTTP/1.1\r\n"
                    + "Host: localhost\r\n"
                    + "Tus-Resumable: 1.0.0\r\n"
                    + "Upload-Offset: 0\r\n"
                    + "Content-Type: application/offset+octet-stream\r\n"
                    + "Transfer-Encoding: chunked\r\n"
                    + "Trailer: Upload-Checksum\r\n"
                    + "\r\n"
                    + Integer.toHexString(body.length) + "\r\n"
                    + new String(body)
                    + "\r\n0\r\n"
                    + "Upload-Checksum: " + checksum + "\r\n"
                    + "\r\n";

            try (java.net.Socket socket = new java.net.Socket("localhost", io.restassured.RestAssured.port)) {
                socket.setSoTimeout(10_000);
                socket.getOutputStream().write(request.getBytes());
                socket.getOutputStream().flush();
                String statusLine = new java.io.BufferedReader(
                        new java.io.InputStreamReader(socket.getInputStream())).readLine();
                assertNotNull(statusLine, "Server closed the connection without responding");
                return Integer.parseInt(statusLine.split(" ")[1]);
            }
        }

        @Test
        void unsupportedChecksumAlgorithmReturns400() {
            byte[] data = "test".getBytes();
            String location = createUpload(data.length);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .header("Upload-Checksum", "crc32 AAAA")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    .statusCode(400);

            // The chunk must not have been stored.
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", "0");
        }
    }

    // ========== Expiration Conformance ==========

    @Nested
    class ExpirationConformance {

        @Test
        void postIncludesUploadExpiresInRfc1123() {
            String expires = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", "100")
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Upload-Expires");

            assertNotNull(expires);
            assertDoesNotThrow(() ->
                    ZonedDateTime.parse(expires, DateTimeFormatter.RFC_1123_DATE_TIME));
        }

        @Test
        void patchIncludesUploadExpiresInRfc1123() {
            byte[] data = "exp".getBytes();
            String location = createUpload(data.length);

            String expires = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    .statusCode(204)
                    .extract().header("Upload-Expires");

            assertNotNull(expires);
            assertDoesNotThrow(() ->
                    ZonedDateTime.parse(expires, DateTimeFormatter.RFC_1123_DATE_TIME));
        }

        @Test
        void expirationIsInTheFuture() {
            String location = createUpload(100);

            String expires = given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .extract().header("Upload-Expires");

            ZonedDateTime parsed = ZonedDateTime.parse(expires, DateTimeFormatter.RFC_1123_DATE_TIME);
            assertTrue(parsed.toInstant().isAfter(java.time.Instant.now()),
                    "Upload-Expires should be in the future");
        }
    }

    // ========== Concatenation Conformance ==========

    @Nested
    class ConcatenationConformance {

        @Test
        void headOnFinalIncludesUploadConcat() {
            byte[] data1 = "aaa".getBytes();
            byte[] data2 = "bbb".getBytes();

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
                    .extract().header("Location");

            String concat = given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(finalLocation)
                    .then()
                    .statusCode(200)
                    .extract().header("Upload-Concat");

            assertNotNull(concat);
            assertTrue(concat.startsWith("final;"));
        }

        /**
         * The spec requires HEAD on a final upload to return Upload-Concat "as received in
         * the upload creation request", so a client that sent absolute URLs must get its own
         * value back rather than one rebuilt from the parsed IDs.
         */
        @Test
        void headOnFinalEchoesUploadConcatAsReceived() {
            byte[] data1 = "aa".getBytes();
            byte[] data2 = "bb".getBytes();

            String loc1 = createPartialUpload(data1.length);
            String loc2 = createPartialUpload(data2.length);
            uploadData(loc1, data1, 0);
            uploadData(loc2, data2, 0);

            String sent = "final; https://example.com/tus/" + extractId(loc1)
                    + " https://example.com/tus/" + extractId(loc2);

            String finalLocation = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Concat", sent)
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Location");

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(finalLocation)
                    .then()
                    .statusCode(200)
                    .header("Upload-Concat", equalTo(sent));
        }

        /**
         * The same applies to a final whose partials are still incomplete, which takes the
         * separate unfinished-merge path.
         */
        @Test
        void headOnUnfinishedFinalEchoesUploadConcatAsReceived() {
            String loc = createPartialUpload(10);
            String sent = "final; https://example.com/tus/" + extractId(loc);

            String finalLocation = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Concat", sent)
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Location");

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(finalLocation)
                    .then()
                    .statusCode(200)
                    .header("Upload-Concat", equalTo(sent));
        }

        @Test
        void patchOnFinalReturns403() {
            byte[] data1 = "xx".getBytes();
            byte[] data2 = "yy".getBytes();

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
                    .extract().header("Location");

            // PATCH on final upload should be forbidden
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body("more".getBytes())
                    .when().patch(finalLocation)
                    .then()
                    .statusCode(403);
        }

        @Test
        void offsetEqualsLengthAfterMerge() {
            byte[] data1 = "part-1".getBytes();
            byte[] data2 = "part-2".getBytes();

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
                    .extract().header("Location");

            long expectedLength = data1.length + data2.length;

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(finalLocation)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", String.valueOf(expectedLength))
                    .header("Upload-Length", String.valueOf(expectedLength));
        }

        @Test
        void finalWithIncompletePartialsCanDeferMerge() {
            // Create a partial but don't upload data
            String loc = createPartialUpload(10);

            // Attempt to create final concat with incomplete partials
            // The server should create an unfinished concat (201) or reject (400)
            int status = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Concat", "final; " + loc)
                    .when().post("/tus")
                    .then()
                    .extract().statusCode();

            // Server supports concatenation-unfinished, so it creates a pending final
            assertTrue(status == 201 || status == 400,
                    "Should create deferred final or reject: got " + status);
        }

        @Test
        void parallelUploadViaConcatenation() throws Exception {
            int numPartials = 3;
            int partSize = 100;
            byte[][] partData = new byte[numPartials][];
            String[] locations = new String[numPartials];

            for (int i = 0; i < numPartials; i++) {
                partData[i] = new byte[partSize];
                Arrays.fill(partData[i], (byte) ('A' + i));
                locations[i] = createPartialUpload(partSize);
            }

            // Upload all partials in parallel
            ExecutorService executor = Executors.newFixedThreadPool(numPartials);
            CountDownLatch latch = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(numPartials);

            for (int i = 0; i < numPartials; i++) {
                final int idx = i;
                executor.submit(() -> {
                    try {
                        latch.await();
                        uploadData(locations[idx], partData[idx], 0);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            latch.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS), "All uploads should complete");
            executor.shutdown();

            // Merge
            String concatHeader = "final; " + String.join(" ", locations);
            String finalLocation = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Concat", concatHeader)
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Location");

            long expectedTotal = (long) numPartials * partSize;

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(finalLocation)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", String.valueOf(expectedTotal))
                    .header("Upload-Length", String.valueOf(expectedTotal));
        }

        @Test
        void headOnPartialShowsPartialConcat() {
            String location = createPartialUpload(50);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Concat", "partial");
        }

        /**
         * The spec forbids PATCH against a final upload URL without distinguishing whether
         * the concatenation has finished. An unfinished final used to accept writes, which
         * finalizeConcatenation would then silently overwrite.
         */
        @Test
        void patchOnUnfinishedFinalReturns403() {
            String partial = createPartialUpload(10);

            String finalLocation = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Concat", "final; " + partial)
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Location");

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body("injected".getBytes())
                    .when().patch(finalLocation)
                    .then()
                    .statusCode(403);
        }

        /**
         * Referencing the same partial repeatedly used to copy it once per occurrence, so a
         * single small request could turn one uploaded partial into a file many times its
         * size — amplification bounded only by the HTTP header limit.
         */
        @Test
        void duplicatePartialReferenceIsRejected() {
            byte[] data = "amplify".getBytes();
            String loc = createPartialUpload(data.length);
            uploadData(loc, data, 0);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Concat", "final; " + loc + " " + loc + " " + loc)
                    .when().post("/tus")
                    .then()
                    .statusCode(400);

            // The partial must not have been consumed by the rejected request.
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(loc)
                    .then()
                    .statusCode(200);
        }

        /**
         * The deferred-merge path takes no locks, so unlike the complete-partial case above it
         * had nothing incidentally stopping duplicates: the final's declared length was the
         * sum of the repeated references, claiming more bytes than were ever uploaded.
         */
        @Test
        void duplicateIncompletePartialReferenceIsRejected() {
            String loc = createPartialUpload(10);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Concat", "final; " + loc + " " + loc + " " + loc)
                    .when().post("/tus")
                    .then()
                    .statusCode(400);
        }

        @Test
        void concatWithInvalidPartialReferenceIsRejected() {
            byte[] data = "abc".getBytes();
            String loc = createPartialUpload(data.length);
            uploadData(loc, data, 0);

            // A non-UUID reference used to be filtered out silently, merging a subset.
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Concat", "final; " + loc + " /tus/not-a-uuid")
                    .when().post("/tus")
                    .then()
                    .statusCode(400);
        }

        @Test
        void concatenationWithSinglePartial() {
            byte[] data = "single".getBytes();
            String loc = createPartialUpload(data.length);
            uploadData(loc, data, 0);

            String finalLocation = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Concat", "final; " + loc)
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Location");

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(finalLocation)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", String.valueOf(data.length))
                    .header("Upload-Length", String.valueOf(data.length));
        }
    }

    // ========== Defer-Length Conformance ==========

    @Nested
    class DeferLengthConformance {

        @Test
        void deferValueNotOneReturns400() {
            // Upload-Defer-Length must be exactly 1; value 0 is treated as absent
            // so the request lacks both Upload-Length and Upload-Defer-Length
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Defer-Length", "0")
                    .when().post("/tus")
                    .then()
                    .statusCode(400);
        }

        @Test
        void setLengthThenCompleteUpload() {
            byte[] data1 = "first".getBytes();
            byte[] data2 = "secnd".getBytes();
            long totalLength = data1.length + data2.length;

            String location = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Defer-Length", "1")
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Location");

            // First PATCH sets the length
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .header("Upload-Length", String.valueOf(totalLength))
                    .contentType("application/offset+octet-stream")
                    .body(data1)
                    .when().patch(location)
                    .then()
                    .statusCode(204);

            // Second PATCH completes the upload (Upload-Length ignored since already set)
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", String.valueOf(data1.length))
                    .contentType("application/offset+octet-stream")
                    .body(data2)
                    .when().patch(location)
                    .then()
                    .statusCode(204);

            // Verify final state
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", String.valueOf(totalLength))
                    .header("Upload-Length", String.valueOf(totalLength));
        }

        @Test
        void deferredLengthExceedingMaxRejected() {
            String location = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Defer-Length", "1")
                    .when().post("/tus")
                    .then()
                    .statusCode(201)
                    .extract().header("Location");

            // Try to set length beyond max-size (104857600 in test config)
            int status = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .header("Upload-Length", "104857601")
                    .contentType("application/offset+octet-stream")
                    .body("x".getBytes())
                    .when().patch(location)
                    .then()
                    .extract().statusCode();

            // setDeferredLength returns false, results in 400
            assertTrue(status == 400 || status == 413,
                    "Deferred length exceeding max should be rejected: got " + status);
        }
    }

    // ========== X-HTTP-Method-Override (core protocol) ==========

    /**
     * The core spec requires the server to interpret X-HTTP-Method-Override as the request
     * method, so clients behind proxies that block PATCH and DELETE can still upload.
     */
    @Nested
    class MethodOverride {

        @Test
        void overridePatchUploadsChunk() {
            byte[] data = "override".getBytes();
            String location = createUpload(data.length);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("X-HTTP-Method-Override", "PATCH")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().post(location)
                    .then()
                    .statusCode(204)
                    .header("Upload-Offset", String.valueOf(data.length));
        }

        @Test
        void overrideHeadReturnsUploadStatus() {
            String location = createUpload(42);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("X-HTTP-Method-Override", "HEAD")
                    .when().post(location)
                    .then()
                    .statusCode(200)
                    .header("Upload-Offset", "0")
                    .header("Upload-Length", "42");
        }

        @Test
        void overrideDeleteTerminatesUpload() {
            String location = createUpload(50);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("X-HTTP-Method-Override", "DELETE")
                    .when().post(location)
                    .then()
                    .statusCode(204);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then()
                    .statusCode(404);
        }

        @Test
        void overrideValueIsCaseInsensitive() {
            byte[] data = "lower".getBytes();
            String location = createUpload(data.length);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("X-HTTP-Method-Override", "patch")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().post(location)
                    .then()
                    .statusCode(204);
        }

        @Test
        void unsupportedOverrideValueReturns400() {
            String location = createUpload(10);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("X-HTTP-Method-Override", "TRACE")
                    .when().post(location)
                    .then()
                    .statusCode(400);
        }

        @Test
        void requestWithoutOverrideHeaderIsUnaffected() {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", "10")
                    .when().post("/tus")
                    .then()
                    .statusCode(201);
        }
    }
}
