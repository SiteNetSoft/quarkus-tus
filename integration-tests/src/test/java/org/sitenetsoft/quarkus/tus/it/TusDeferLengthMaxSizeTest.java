package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;

/**
 * A deferred-length upload is not exempt from {@code quarkus.tus.max-size}: skipping the
 * chunk-boundary check while the length is still unknown (see
 * {@code TusUploadResource#patchUnderLockValidated}) must not become a way to PATCH data
 * forever without ever declaring a length. The running offset plus each incoming chunk is
 * checked against the server-wide cap directly.
 *
 * <p>Run against its own small {@code max-size} (rather than the module's real 100 MiB test
 * config) so the overflow can be reached with a few real bytes instead of gigabytes of PATCH
 * traffic -- the same reasoning {@link TusConcatLimitTest} and
 * {@link TusStreamingMemoryTest} use for their own limits.
 */
@QuarkusTest
@TestProfile(TusDeferLengthMaxSizeTest.SmallMaxSizeProfile.class)
class TusDeferLengthMaxSizeTest {

    private static final long MAX_SIZE = 500;

    public static class SmallMaxSizeProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.tus.max-size", String.valueOf(MAX_SIZE),
                    // Boot-time validation requires max-chunk-size <= max-size.
                    "quarkus.tus.max-chunk-size", String.valueOf(MAX_SIZE));
        }
    }

    @Test
    void deferredDataChunkExceedingMaxSizeIsRejectedAndOffsetStaysPut() {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Defer-Length", "1")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        // First chunk comfortably within the cap: the length stays deferred, offset advances.
        byte[] firstChunk = new byte[300];
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(firstChunk)
                .when().patch(location)
                .then()
                .statusCode(204)
                .header("Upload-Offset", String.valueOf(firstChunk.length));

        // Second chunk: still no Upload-Length, and 300 + 300 > 500 -- must be rejected before
        // ever being written, not accepted because the length is still unknown.
        byte[] secondChunk = new byte[300];
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", String.valueOf(firstChunk.length))
                .contentType("application/offset+octet-stream")
                .body(secondChunk)
                .when().patch(location)
                .then()
                .statusCode(413);

        // The rejected chunk must not have moved the offset.
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", String.valueOf(firstChunk.length))
                .header("Upload-Defer-Length", "1");
    }
}
