package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
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

@QuarkusTest
class TusSseTest {

    @Test
    void testSseEndpointExists() throws Exception {
        // Create an upload first to get a valid ID
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .extract().header("Location");

        String uploadId = location.substring(location.lastIndexOf('/') + 1);

        // Use raw HttpURLConnection with read timeout to verify SSE endpoint
        int port = io.restassured.RestAssured.port;
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + port + "/tus/events/" + uploadId).toURL().openConnection();
        conn.setRequestProperty("Accept", "text/event-stream");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);

        try {
            assertEquals(200, conn.getResponseCode());
            // Read first chunk of data
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                StringBuilder sb = new StringBuilder();
                // Read with timeout - just get the first few lines
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - start < 2000) {
                    if (reader.ready()) {
                        String line = reader.readLine();
                        if (line != null) {
                            sb.append(line).append("\n");
                        }
                    } else {
                        if (!sb.isEmpty()) break; // Got some data, stop
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

        // Create upload
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        String uploadId = location.substring(location.lastIndexOf('/') + 1);

        // Connect to progress SSE endpoint
        int port = io.restassured.RestAssured.port;
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + port + "/tus/progress/" + uploadId).toURL().openConnection();
        conn.setRequestProperty("Accept", "text/event-stream");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(5000);

        try {
            assertEquals(200, conn.getResponseCode());

            // Read SSE events in background
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

            // Give SSE connection time to register
            Thread.sleep(500);

            // Upload data
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Offset", "0")
                    .contentType("application/offset+octet-stream")
                    .body(data)
                    .when().patch(location)
                    .then()
                    .statusCode(204);

            // Verify progress event received
            String received = sseEvents.get(5, TimeUnit.SECONDS);
            assertTrue(received.contains("upload-progress"),
                    "Should receive upload-progress event, got: " + received);
        } finally {
            conn.disconnect();
        }
    }
}
