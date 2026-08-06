package org.sitenetsoft.quarkus.tus.it;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

/**
 * SSE endpoint tests (HTTP-only, no CDI injection required).
 * Extended by both {@code @QuarkusTest} and {@code @QuarkusIntegrationTest} subclasses.
 */
abstract class TusSseTestBase {

    @Test
    void testSseEndpointExists() throws Exception {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .extract().header("Location");

        String uploadId = location.substring(location.lastIndexOf('/') + 1);

        int port = io.restassured.RestAssured.port;
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + port + "/tus/events/" + uploadId).toURL().openConnection();
        conn.setRequestProperty("Accept", "text/event-stream");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);

        try {
            assertEquals(200, conn.getResponseCode());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                StringBuilder sb = new StringBuilder();
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - start < 2000) {
                    if (reader.ready()) {
                        String line = reader.readLine();
                        if (line != null) {
                            sb.append(line).append("\n");
                        }
                    } else {
                        if (!sb.isEmpty()) break;
                        Thread.sleep(100);
                    }
                }
                String body = sb.toString();
                assertTrue(body.contains("connected"), "SSE stream should contain 'connected' event, got: " + body);
            }
        } finally {
            conn.disconnect();
        }
    }

    @Test
    void testProgressEndpointDeliversEvents() throws Exception {
        byte[] data = "progress test data".getBytes();

        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        String uploadId = location.substring(location.lastIndexOf('/') + 1);

        int port = io.restassured.RestAssured.port;
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + port + "/tus/progress/" + uploadId).toURL().openConnection();
        conn.setRequestProperty("Accept", "text/event-stream");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(5000);

        try {
            assertEquals(200, conn.getResponseCode());

            CompletableFuture<String> sseEvents = CompletableFuture.supplyAsync(() -> {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream()))) {
                    StringBuilder sb = new StringBuilder();
                    long start = System.currentTimeMillis();
                    while (System.currentTimeMillis() - start < 4000) {
                        if (reader.ready()) {
                            String line = reader.readLine();
                            if (line != null) sb.append(line).append("\n");
                            if (sb.toString().contains("upload-progress")) break;
                        } else {
                            Thread.sleep(100);
                        }
                    }
                    return sb.toString();
                } catch (Exception e) {
                    return "ERROR: " + e.getMessage();
                }
            });

            Thread.sleep(500);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    .statusCode(204);

            String received = sseEvents.get(5, TimeUnit.SECONDS);
            assertTrue(received.contains("upload-progress"),
                    "Should receive upload-progress event, got: " + received);
        } finally {
            conn.disconnect();
        }
    }

    /**
     * The event stream must deliver the lifecycle the documentation describes, not just its opening
     * handshake. It previously sent {@code connected} and then nothing at all, because the sink map
     * it registers into was never written to by anything.
     */
    @Test
    void testEventStreamDeliversProgressAndComplete() throws Exception {
        byte[] data = "lifecycle event payload".getBytes();

        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        String uploadId = location.substring(location.lastIndexOf('/') + 1);

        int port = io.restassured.RestAssured.port;
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + port + "/tus/events/" + uploadId).toURL().openConnection();
        conn.setRequestProperty("Accept", "text/event-stream");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(4000);

        try {
            assertEquals(200, conn.getResponseCode());

            // Blocking reads rather than ready() polling: only a null from readLine tells us the
            // server closed the stream, and that closure is what releases the sink.
            CompletableFuture<String> events = CompletableFuture.supplyAsync(() -> {
                StringBuilder sb = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line).append("\n");
                    }
                    sb.append("STREAM-CLOSED\n");
                } catch (Exception e) {
                    // Keep what we read: a timeout here means the stream stayed open, and the
                    // assertions should say that rather than appear to lose earlier events.
                    sb.append("NOT-CLOSED: ").append(e.getMessage()).append("\n");
                }
                return sb.toString();
            });

            Thread.sleep(500);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    .statusCode(204);

            String received = events.get(12, TimeUnit.SECONDS);
            assertTrue(received.contains("connected"),
                    "Stream should open with a connected event, got: " + received);
            assertTrue(received.contains("event:progress"),
                    "Stream should deliver a progress event, got: " + received);
            assertTrue(received.contains("\"bytesUploaded\": " + data.length),
                    "Progress should report bytes uploaded, got: " + received);
            assertTrue(received.contains("event:complete"),
                    "Stream should deliver a complete event, got: " + received);
            assertTrue(received.contains("STREAM-CLOSED"),
                    "Server should close the stream after complete, releasing the sink; got: " + received);
        } finally {
            conn.disconnect();
        }
    }

    /**
     * The chunk that completes an upload must still produce a progress event. The store clears the
     * progress entry when the upload completes, and that used to happen before the event was sent,
     * so a client watching the stream saw progress stall just short of 100% and never learned the
     * upload had finished.
     */
    @Test
    void testProgressReaches100PercentOnCompletion() throws Exception {
        byte[] data = "twenty bytes exactly".getBytes();

        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        String uploadId = location.substring(location.lastIndexOf('/') + 1);

        int port = io.restassured.RestAssured.port;
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + port + "/tus/progress/" + uploadId).toURL().openConnection();
        conn.setRequestProperty("Accept", "text/event-stream");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(8000);

        try {
            assertEquals(200, conn.getResponseCode());

            CompletableFuture<String> sseEvents = CompletableFuture.supplyAsync(() -> {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream()))) {
                    StringBuilder sb = new StringBuilder();
                    long start = System.currentTimeMillis();
                    while (System.currentTimeMillis() - start < 6000) {
                        if (reader.ready()) {
                            String line = reader.readLine();
                            if (line != null) sb.append(line).append("\n");
                            if (sb.toString().contains("\"percentage\": 100")) break;
                        } else {
                            Thread.sleep(100);
                        }
                    }
                    return sb.toString();
                } catch (Exception e) {
                    return "ERROR: " + e.getMessage();
                }
            });

            Thread.sleep(500);

            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    .statusCode(204);

            String received = sseEvents.get(8, TimeUnit.SECONDS);
            assertTrue(received.contains("\"percentage\": 100"),
                    "Completing chunk should emit a 100% progress event, got: " + received);
            assertTrue(received.contains("\"uploadedBytes\": " + data.length),
                    "Final event should report all bytes uploaded, got: " + received);
        } finally {
            conn.disconnect();
        }
    }
}
