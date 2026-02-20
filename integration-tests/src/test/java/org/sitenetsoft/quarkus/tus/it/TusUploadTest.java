package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.Response;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.Base64;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class TusUploadTest {

    @Inject
    TusTestObserver observer;

    @BeforeEach
    void setUp() {
        observer.reset();
    }

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
}
