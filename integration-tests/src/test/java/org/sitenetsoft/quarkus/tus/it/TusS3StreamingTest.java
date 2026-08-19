package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.Random;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Does a chunk really stream client → server → S3? The store counts parts and the most bytes it
 * ever held; a 12 MB PATCH must arrive as three parts with the store never holding more than
 * about one part, and 400 MB must go through the 512 MB test JVM the same way.
 * <p>
 * Runs only with {@code TUS_S3_ENDPOINT} set (MinIO: {@code podman run -p 9000:9000
 * -e MINIO_ROOT_USER=minio -e MINIO_ROOT_PASSWORD=minio12345 quay.io/minio/minio server /data}).
 */
@QuarkusTest
@TestProfile(S3UploadStoreContractTest.S3Profile.class)
@EnabledIfEnvironmentVariable(named = "TUS_S3_ENDPOINT", matches = ".+")
class TusS3StreamingTest {

    @Inject
    UploadStore uploadStore;

    private S3UploadStore store() {
        return (S3UploadStore) uploadStore;
    }

    @BeforeEach
    void reset() {
        store().resetMetrics();
    }

    private static String createUpload(long size) {
        return given().header("Tus-Resumable", "1.0.0").header("Upload-Length", String.valueOf(size))
                .when().post("/tus").then().statusCode(201).extract().header("Location");
    }

    private static String createPartialUpload(long size) {
        return given().header("Tus-Resumable", "1.0.0").header("Upload-Length", String.valueOf(size))
                .header("Upload-Concat", "partial")
                .when().post("/tus").then().statusCode(201).extract().header("Location");
    }

    /**
     * PATCHes with curl from a separate process: RestAssured/Apache HttpClient buffers a large
     * body ~100x over in the test JVM (measured 1.2 GB for 12 MB), which would drown the
     * measurement — and OOM the 512 MB test worker — before the server side is even involved.
     */
    private static void uploadData(String location, byte[] data, long offset) {
        try {
            java.nio.file.Path file = java.nio.file.Files.createTempFile("tus-s3", ".bin");
            java.nio.file.Files.write(file, data);
            try {
                curlPatch(location, file, offset, null, 204);
            } finally {
                java.nio.file.Files.delete(file);
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static String curlPatch(String location, java.nio.file.Path file, long offset, String checksum, int expected)
            throws Exception {
        java.util.List<String> cmd = new java.util.ArrayList<>(java.util.List.of("curl", "-s", "-o", "/dev/null",
                "-D", "-", "-X", "PATCH", "-H", "Tus-Resumable: 1.0.0", "-H", "Upload-Offset: " + offset,
                "-H", "Content-Type: application/offset+octet-stream", "-H", "Expect:"));
        if (checksum != null) {
            cmd.add("-H"); cmd.add("Upload-Checksum: " + checksum);
        }
        cmd.add("--data-binary"); cmd.add("@" + file);
        cmd.add("http://localhost:" + io.restassured.RestAssured.port + location);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String headers = new String(p.getInputStream().readAllBytes());
        p.waitFor();
        String statusLine = headers.lines().findFirst().orElse("");
        assertTrue(statusLine.contains(" " + expected + " "), "expected " + expected + " but got: " + statusLine);
        return headers;
    }

    private static String extractId(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }

    private static byte[] random(int size) {
        byte[] data = new byte[size];
        new Random(42).nextBytes(data);
        return data;
    }

    @Test
    void twelveMegabytePatchStreamsAsThreeParts() {
        byte[] data = random(12 * 1024 * 1024);
        String location = createUpload(data.length);
        String id = extractId(location);

        uploadData(location, data, 0);

        assertArrayEquals(data, store().objectContent(id), "object in S3 must equal what was sent");
        assertEquals(3, store().partsUploaded.get(), "5 MB + 5 MB + 2 MB tail");
        long held = store().maxBufferedBytes.get();
        assertTrue(held < 6L * 1024 * 1024, "store held " + held + " bytes at peak — that is not streaming");
        System.out.println("peak buffered bytes: " + held + " for a " + data.length + " byte chunk");
    }

    /** 400 MB through a 512 MB test JVM into S3: 80 parts, one part in memory. */
    @Test
    void fourHundredMegabytesStreamThroughABoundedHeap() throws Exception {
        long size = 400L * 1024 * 1024;
        java.nio.file.Path file = java.nio.file.Files.createTempFile("tus-s3", ".bin");
        new ProcessBuilder("sh", "-c", "head -c " + size + " /dev/urandom > " + file).inheritIO().start().waitFor();
        try {
            String location = given().header("Tus-Resumable", "1.0.0").header("Upload-Length", String.valueOf(size))
                    .when().post("/tus").then().statusCode(201).extract().header("Location");
            String headers = curlPatch(location, file, 0, null, 204);
            assertTrue(headers.toLowerCase().contains("upload-offset: " + size), headers);
            assertEquals(80, store().partsUploaded.get());
            long held = store().maxBufferedBytes.get();
            assertTrue(held < 6L * 1024 * 1024, "store held " + held + " bytes at peak");
            System.out.println("400MB: peak buffered " + held + " bytes, heap max " + Runtime.getRuntime().maxMemory() / 1_000_000 + " MB");
        } finally {
            java.nio.file.Files.delete(file);
        }
    }

    @Test
    void smallChunksAreBufferedIntoOneFinalPart() {
        byte[] data = random(5 * 1024);
        String location = createUpload(data.length);
        String id = extractId(location);
        for (int i = 0; i < 5; i++) {
            byte[] chunk = java.util.Arrays.copyOfRange(data, i * 1024, (i + 1) * 1024);
            uploadData(location, chunk, i * 1024L);
        }
        assertArrayEquals(data, store().objectContent(id));
        assertEquals(1, store().partsUploaded.get(), "everything under 5 MB becomes the single final part");
    }

    @Test
    void checksumMismatchOnALargeChunkDropsItsParts() throws Exception {
        byte[] data = random(6 * 1024 * 1024);
        String location = createUpload(data.length);
        String id = extractId(location);

        java.nio.file.Path file = java.nio.file.Files.createTempFile("tus-s3", ".bin");
        java.nio.file.Files.write(file, data);
        try {
            String wrong = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1").digest("nope".getBytes()));
            curlPatch(location, file, 0, "sha1 " + wrong, 460);
            assertEquals(1, store().partsUploaded.get(), "the first 5 MB part had already gone to S3 before the digest was known");

            // Offset untouched; the retry with the right digest completes with the right content.
            String right = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1").digest(data));
            String headers = curlPatch(location, file, 0, "sha1 " + right, 204);
            assertTrue(headers.toLowerCase().contains("upload-offset: " + data.length), headers);
        } finally {
            java.nio.file.Files.delete(file);
        }
        assertArrayEquals(data, store().objectContent(id));
    }

    @Test
    void concatenationOfPartialsStreamsThroughS3() {
        byte[] a = random(3 * 1024 * 1024);
        byte[] b = random(4 * 1024 * 1024);
        String la = createPartialUpload(a.length);
        String lb = createPartialUpload(b.length);
        uploadData(la, a, 0);
        uploadData(lb, b, 0);

        String finalLocation = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Concat", "final;" + la + " " + lb)
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");
        byte[] expected = new byte[a.length + b.length];
        System.arraycopy(a, 0, expected, 0, a.length);
        System.arraycopy(b, 0, expected, a.length, b.length);
        assertArrayEquals(expected, store().objectContent(extractId(finalLocation)));
    }
}
