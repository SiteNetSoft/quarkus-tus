package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Duration;

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
}
