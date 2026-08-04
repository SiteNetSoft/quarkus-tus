package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The progress and event streams expose an upload's size and live byte counts, and registering
 * a sink for an upload displaces any existing subscriber. Knowing an upload ID must therefore
 * not be enough to watch — or hijack — someone else's upload.
 */
@QuarkusTest
@TestProfile(TusSseOwnershipTest.AuthEnabledProfile.class)
class TusSseOwnershipTest {

    public static class AuthEnabledProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.tus.auth-enabled", "true",
                    "quarkus.tus.sse-enabled", "true",
                    "quarkus.http.auth.basic", "true",
                    "quarkus.security.users.embedded.enabled", "true",
                    "quarkus.security.users.embedded.plain-text", "true",
                    "quarkus.security.users.embedded.users.alice", "alice-pw",
                    "quarkus.security.users.embedded.users.bob", "bob-pw",
                    "quarkus.security.users.embedded.roles.alice", "user",
                    "quarkus.security.users.embedded.roles.bob", "user");
        }
    }

    private static RequestSpecification asAlice() {
        return given().auth().preemptive().basic("alice", "alice-pw")
                .header("Tus-Resumable", "1.0.0");
    }

    private static String aliceCreatesUpload() {
        String location = asAlice()
                .header("Upload-Length", "100")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");
        return location.substring(location.lastIndexOf('/') + 1);
    }

    /** Opens an SSE stream as the given user and returns the HTTP status, without consuming it. */
    private static int streamStatus(String path, String user, String password) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + RestAssured.port + path).toURL().openConnection();
        conn.setRequestProperty("Accept", "text/event-stream");
        conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);
        try {
            return conn.getResponseCode();
        } finally {
            conn.disconnect();
        }
    }

    @Test
    void ownerCanStreamProgress() throws Exception {
        String uploadId = aliceCreatesUpload();
        assertEquals(200, streamStatus("/tus/progress/" + uploadId, "alice", "alice-pw"));
    }

    @Test
    void ownerCanStreamEvents() throws Exception {
        String uploadId = aliceCreatesUpload();
        assertEquals(200, streamStatus("/tus/events/" + uploadId, "alice", "alice-pw"));
    }

    @Test
    void otherUserCannotStreamProgress() throws Exception {
        String uploadId = aliceCreatesUpload();
        assertEquals(404, streamStatus("/tus/progress/" + uploadId, "bob", "bob-pw"));
    }

    @Test
    void otherUserCannotStreamEvents() throws Exception {
        String uploadId = aliceCreatesUpload();
        assertEquals(404, streamStatus("/tus/events/" + uploadId, "bob", "bob-pw"));
    }

    @Test
    void unknownUploadIsNotStreamable() throws Exception {
        assertEquals(404, streamStatus(
                "/tus/progress/550e8400-e29b-41d4-a716-446655440000", "alice", "alice-pw"));
        assertEquals(404, streamStatus(
                "/tus/events/550e8400-e29b-41d4-a716-446655440000", "alice", "alice-pw"));
    }

    @Test
    void malformedUploadIdIsRejectedOnProgressStream() throws Exception {
        assertEquals(404, streamStatus("/tus/progress/not-a-uuid", "alice", "alice-pw"));
    }
}
