package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.UploadProgressService;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class TusSseTest extends TusSseTestBase {

    @Inject
    UploadProgressService uploadProgressService;

    /** Reads /tus/progress/{id} until {@code marker} appears {@code times} or the time is up; returns what was read. */
    private static CompletableFuture<String> collectUntil(HttpURLConnection conn, String marker, int times) {
        return CompletableFuture.supplyAsync(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                StringBuilder sb = new StringBuilder();
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - start < 6000) {
                    if (reader.ready()) {
                        String line = reader.readLine();
                        if (line != null) sb.append(line).append("\n");
                        if (count(sb.toString(), marker) >= times) break;
                    } else {
                        Thread.sleep(100);
                    }
                }
                return sb.toString();
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }
        });
    }

    private static int count(String haystack, String needle) {
        int n = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) n++;
        return n;
    }

    private static String createUpload(long size) {
        return given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(size))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");
    }

    private static void uploadData(String location, byte[] data, long offset) {
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", String.valueOf(offset))
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);
    }

    private static HttpURLConnection openProgressStream(String uploadId) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + io.restassured.RestAssured.port + "/tus/progress/" + uploadId).toURL().openConnection();
        conn.setRequestProperty("Accept", "text/event-stream");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(8000);
        assertEquals(200, conn.getResponseCode());
        return conn;
    }

    /**
     * Progress entries live in memory; after a restart the store still knows the upload but the
     * progress service does not. The chunk that completes such an upload must still tell the
     * stream it reached 100%, or a watcher stalls forever on the previous chunk.
     */
    @Test
    void testCompletingChunkEmits100PercentWithoutAProgressEntry() throws Exception {
        byte[] data = "twenty bytes exactly".getBytes();
        String location = createUpload(data.length);
        String uploadId = location.substring(location.lastIndexOf('/') + 1);
        uploadProgressService.finishUpload(uploadId); // what a restart does to the entry

        HttpURLConnection conn = openProgressStream(uploadId);
        try {
            CompletableFuture<String> events = collectUntil(conn, "\"percentage\": 100", 1);
            Thread.sleep(500);
            uploadData(location, data, 0);
            String received = events.get(8, TimeUnit.SECONDS);
            assertTrue(received.contains("\"percentage\": 100"), "expected a 100% event, got: " + received);
            assertTrue(received.contains("\"uploadedBytes\": " + data.length), received);
        } finally {
            conn.disconnect();
        }
    }

    /** A zero-length PATCH is how some clients poll; it should report where the upload stands. */
    @Test
    void testZeroLengthPatchEmitsCurrentProgress() throws Exception {
        String location = createUpload(20);
        String uploadId = location.substring(location.lastIndexOf('/') + 1);
        uploadData(location, new byte[10], 0);

        HttpURLConnection conn = openProgressStream(uploadId);
        try {
            // The stream sends a snapshot on connect; the PATCH must add a second event.
            CompletableFuture<String> events = collectUntil(conn, "\"uploadedBytes\": 10", 2);
            Thread.sleep(500);
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "10")
                    .contentType("application/offset+octet-stream")
                    .body(new byte[0])
                    .when().patch(location)
                    .then()
                    .statusCode(204);
            String received = events.get(8, TimeUnit.SECONDS);
            assertEquals(2, count(received, "\"uploadedBytes\": 10"), "expected the snapshot plus one event from the PATCH, got: " + received);
        } finally {
            conn.disconnect();
        }
    }
}
