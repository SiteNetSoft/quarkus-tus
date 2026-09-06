package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.UploadExpirationScheduler;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.sse.TusSseService;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Every SSE sink must go when its upload goes. A sink is only ever dropped by the code that
 * sends to it, so a stream nobody writes to after the upload is gone stays registered — and its
 * connection open — forever. Completion used to close the events stream but not the progress
 * stream; expiry and the scheduled cleanups closed neither.
 * <p>
 * Closure is asserted with blocking reads: only a {@code null} from {@code readLine} proves the
 * server closed the connection. {@code ready()} polling cannot tell a closed stream from a
 * quiet one.
 */
@QuarkusTest
class TusSseLifecycleTest {

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

    private static String extractId(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }

    private static String createPartialUpload(long size) {
        return given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(size))
                .header("Upload-Concat", "partial")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");
    }

    @Inject
    UploadStore uploadStore;

    @Inject
    UploadExpirationScheduler scheduler;

    @Inject
    TusSseService sseService;

    private static HttpURLConnection open(String path, String uploadId) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + io.restassured.RestAssured.port + path + "/" + uploadId)
                .toURL().openConnection();
        conn.setRequestProperty("Accept", "text/event-stream");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(8000);
        assertEquals(200, conn.getResponseCode());
        return conn;
    }

    /** Reads to end-of-stream on another thread; the future completes only when the server closes. */
    private static CompletableFuture<String> drain(HttpURLConnection conn) {
        return CompletableFuture.supplyAsync(() -> {
            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append("\n");
                }
                sb.append("STREAM-CLOSED\n");
            } catch (Exception e) {
                sb.append("NOT-CLOSED: ").append(e.getMessage()).append("\n");
            }
            return sb.toString();
        });
    }

    private static String closed(CompletableFuture<String> drained, String what) throws Exception {
        String received = drained.get(12, TimeUnit.SECONDS);
        assertTrue(received.contains("STREAM-CLOSED"), what + " stream was not closed; got: " + received);
        return received;
    }

    private void expire(String id) {
        UploadInfo info = Stores.find(uploadStore, id).orElseThrow();
        info.setExpiresAt(Instant.now().minusSeconds(1));
        Stores.update(uploadStore, id, info);
    }

    @Test
    void progressStreamClosesWhenTheUploadCompletes() throws Exception {
        byte[] data = "progress closes on completion".getBytes();
        String location = createUpload(data.length);
        HttpURLConnection conn = open("/tus/progress", extractId(location));
        try {
            CompletableFuture<String> drained = drain(conn);
            Thread.sleep(300);
            uploadData(location, data, 0);
            String received = closed(drained, "progress");
            assertTrue(received.contains("\"percentage\": 100"), "100% should still arrive first: " + received);
        } finally {
            conn.disconnect();
        }
    }

    @Test
    void bothStreamsCloseWhenExpiryIsDiscoveredByHead() throws Exception {
        String location = createUpload(100);
        String id = extractId(location);
        HttpURLConnection events = open("/tus/events", id);
        HttpURLConnection progress = open("/tus/progress", id);
        try {
            CompletableFuture<String> eventsDrained = drain(events);
            CompletableFuture<String> progressDrained = drain(progress);
            Thread.sleep(300);
            expire(id);
            given().header("Tus-Resumable", "1.0.0").when().head(location).then().statusCode(410);
            closed(eventsDrained, "events");
            closed(progressDrained, "progress");
        } finally {
            events.disconnect();
            progress.disconnect();
        }
    }

    @Test
    void bothStreamsCloseWhenTheScheduledCleanupRemovesTheUpload() throws Exception {
        String location = createUpload(100);
        String id = extractId(location);
        HttpURLConnection events = open("/tus/events", id);
        HttpURLConnection progress = open("/tus/progress", id);
        try {
            CompletableFuture<String> eventsDrained = drain(events);
            CompletableFuture<String> progressDrained = drain(progress);
            Thread.sleep(300);
            expire(id);
            scheduler.cleanupExpiredUploads();
            assertTrue(Stores.find(uploadStore, id).isEmpty(), "cleanup did not remove the expired upload");
            closed(eventsDrained, "events");
            closed(progressDrained, "progress");
        } finally {
            events.disconnect();
            progress.disconnect();
        }
    }

    /**
     * A partial merged into a final upload is discarded under the concatenation's locks, which
     * is a third discard path; its streams must go the same way.
     */
    @Test
    void partialStreamsCloseWhenItIsMergedAway() throws Exception {
        byte[] data = "part".getBytes();
        String partial = createPartialUpload(data.length);
        String id = extractId(partial);
        uploadData(partial, data, 0);
        HttpURLConnection events = open("/tus/events", id);
        try {
            CompletableFuture<String> drained = drain(events);
            Thread.sleep(300);
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Concat", "final;" + partial)
                    .when().post("/tus")
                    .then()
                    .statusCode(201);
            closed(drained, "partial events");
        } finally {
            events.disconnect();
        }
    }

    /**
     * Hold-open is a promise about completion, not about the upload vanishing: an upload that
     * is discarded before it ever completed has no pipeline running for it, and no backstop
     * timer armed either, so the stream would otherwise stay open for good.
     */
    @Test
    void heldOpenStreamOfAnUploadDiscardedBeforeCompletionIsClosed() throws Exception {
        String location = createUpload(100);
        String id = extractId(location);
        HttpURLConnection events = open("/tus/events", id);
        try {
            CompletableFuture<String> drained = drain(events);
            Thread.sleep(300);
            sseService.holdOpen(id);
            expire(id);
            given().header("Tus-Resumable", "1.0.0").when().head(location).then().statusCode(410);
            closed(drained, "held-open events");
        } finally {
            events.disconnect();
        }
    }
}
