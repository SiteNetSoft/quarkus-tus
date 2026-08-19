package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Chunk bodies stream through the store; they are never buffered whole. This is the test that
 * would notice if that regressed: a chunk larger than the free heap. The Gradle test worker runs
 * with 512 MB, so 400 MB cannot be held — buffered, this OOMs; streamed, it is a 204 and a
 * 400 MiB file.
 * <p>
 * The client is curl in a separate process, deliberately: RestAssured/Apache HttpClient buffers
 * a large body many times over in the test JVM (measured 1.2 GB for 12 MB), which would fail
 * this test for the wrong reason.
 */
@QuarkusTest
@TestProfile(TusStreamingMemoryTest.BigChunkProfile.class)
@EnabledOnOs({OS.LINUX, OS.MAC})
class TusStreamingMemoryTest {

    public static class BigChunkProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.tus.max-chunk-size", String.valueOf(512L * 1024 * 1024),
                    "quarkus.tus.max-size", String.valueOf(1024L * 1024 * 1024),
                    "quarkus.http.limits.max-body-size", "1G");
        }
    }

    @Test
    void chunkLargerThanTheHeapStreamsToDisk() throws Exception {
        long size = 400L * 1024 * 1024;
        // The test JVM already sits at ~300 MB with the app booted; a heap under twice the chunk
        // cannot hold the chunk on top of that, which is what makes this test mean something.
        assertTrue(Runtime.getRuntime().maxMemory() < 2 * size,
                "this test only proves anything if the chunk cannot be buffered in the heap; heap is "
                        + Runtime.getRuntime().maxMemory());

        Path file = Files.createTempFile("tus-big", ".bin");
        try {
            new ProcessBuilder("sh", "-c", "head -c " + size + " /dev/urandom > " + file).inheritIO().start().waitFor();
            String location = given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", String.valueOf(size))
                    .when().post("/tus")
                    .then().statusCode(201)
                    .extract().header("Location");

            Process curl = new ProcessBuilder("curl", "-s", "-o", "/dev/null", "-D", "-", "-X", "PATCH",
                    "-H", "Tus-Resumable: 1.0.0", "-H", "Upload-Offset: 0",
                    "-H", "Content-Type: application/offset+octet-stream", "-H", "Expect:",
                    "--data-binary", "@" + file,
                    "http://localhost:" + io.restassured.RestAssured.port + location)
                    .redirectErrorStream(true).start();
            String headers = new String(curl.getInputStream().readAllBytes());
            curl.waitFor();
            assertTrue(headers.startsWith("HTTP/1.1 204"), headers);
            assertTrue(headers.toLowerCase().contains("upload-offset: " + size), headers);

            given().header("Tus-Resumable", "1.0.0")
                    .when().head(location)
                    .then().statusCode(200)
                    .header("Upload-Offset", String.valueOf(size));
        } finally {
            Files.deleteIfExists(file);
        }
    }
}
