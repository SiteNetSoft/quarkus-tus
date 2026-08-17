package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.util.Set;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

/**
 * How the framework copes with a store that bends the SPI contract. A third-party store is
 * the least tested code in any deployment; the framework must not turn its slips into leaked
 * locks or lost bytes.
 */
@QuarkusTest
@TestProfile(TusFaultyStoreTest.FaultyStoreProfile.class)
class TusFaultyStoreTest extends TusUploadTestBase {

    public static class FaultyStoreProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(FaultyUploadStore.class);
        }
    }

    @Inject
    UploadStore uploadStore;

    private FaultyUploadStore store() {
        return (FaultyUploadStore) uploadStore;
    }

    @BeforeEach
    void resetStore() {
        store().reset();
    }

    /**
     * A stale offset from the store means nothing was staged, and the record's offset is the
     * truth. Aborting "at the caller's offset" would tell the store to roll the upload back
     * below where it really is.
     */
    @Test
    void staleOffsetFromStoreIsNotAbortedAtTheStaleOffset() {
        String location = createUpload(100);
        store().reportStaleOffsetAs.set(40);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(new byte[10])
                .when().patch(location)
                .then()
                .statusCode(409)
                .header("Upload-Offset", "40");

        assertEquals(0, store().abortCalls.get(), "nothing was staged, so nothing to abort");
    }

    /** A store that throws instead of failing the Uni still gets the mapped status on PATCH. */
    @Test
    void synchronousThrowFromStageOnPatchIsMappedNotInternalError() {
        String location = createUpload(100);
        store().throwSyncFromStage.set(true);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(new byte[10])
                .when().patch(location)
                .then()
                .statusCode(404);

        store().throwSyncFromStage.set(false);
        uploadData(location, new byte[10], 0); // lock was released
    }

    /**
     * ...and creation-with-upload must not leak the lock it took before calling the store. The
     * half-created upload is discarded too: the client got an error, not a Location, so it will
     * create again rather than resume.
     */
    @Test
    void synchronousThrowFromStageOnCreationWithUploadReleasesTheLockAndDiscards() {
        store().throwSyncFromStage.set(true);
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .contentType("application/offset+octet-stream")
                .body(new byte[10])
                .when().post("/tus")
                .then()
                .statusCode(404);
        store().throwSyncFromStage.set(false);

        String id = store().lastCreatedId;
        assertNotNull(id);
        assertTrue(store().acquireLock(id), "lock leaked by the failed creation-with-upload");
        store().releaseLock(id);
        assertTrue(store().findUploadInfo(id).isEmpty(), "failed creation-with-upload left its upload behind");
    }

    /**
     * When the client goes away mid-chunk, Quarkus REST cancels the request's pipeline rather
     * than failing it. The store was promised an abort for every stage that does not commit,
     * and it must get one — before the lock is released, so nobody stages into the same place
     * while the abort is still rolling back.
     */
    @Test
    void clientDisconnectMidChunkAbortsTheStagedChunk() throws Exception {
        String location = createUpload(10_000);
        String id = extractId(location);
        int port = Integer.getInteger("quarkus.http.test-port", 8081);
        try (java.net.Socket socket = new java.net.Socket("localhost", port)) {
            java.io.OutputStream out = socket.getOutputStream();
            out.write(("PATCH /tus/" + id + " HTTP/1.1\r\nHost: localhost\r\nTus-Resumable: 1.0.0\r\n"
                    + "Upload-Offset: 0\r\nContent-Type: application/offset+octet-stream\r\n"
                    + "Content-Length: 1000\r\n\r\n").getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            out.write(new byte[400]);
            out.flush();
            Thread.sleep(300);
        }

        long deadline = System.currentTimeMillis() + 5_000;
        while (store().abortCalls.get() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(1, store().abortCalls.get(), "the store was not told to abort the interrupted chunk");
        assertEquals(0, store().findUploadInfo(id).orElseThrow().getOffset());
        uploadData(location, new byte[100], 0); // lock released, upload still usable
    }

    /**
     * The framework cuts the body off at max-chunk-size by failing the stream, but a store (or
     * its SDK) may wrap that failure in its own type. The 413 is the framework's decision from
     * what it counted, not a matter of the store returning the right exception.
     */
    @Test
    void chunkOverrunIsStill413WhenTheStoreWrapsStreamFailures() throws Exception {
        String location = createUpload(10_000);
        String id = extractId(location);
        store().wrapStreamFailures.set(true);
        int port = Integer.getInteger("quarkus.http.test-port", 8081);
        try (java.net.Socket socket = new java.net.Socket("localhost", port)) {
            socket.setSoTimeout(10_000);
            java.io.OutputStream out = socket.getOutputStream();
            out.write(("PATCH /tus/" + id + " HTTP/1.1\r\nHost: localhost\r\nTus-Resumable: 1.0.0\r\n"
                    + "Upload-Offset: 0\r\nContent-Type: application/offset+octet-stream\r\n"
                    + "Transfer-Encoding: chunked\r\n\r\n").getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            byte[] part = new byte[600];
            for (int i = 0; i < 2; i++) { // 1200 bytes > max-chunk-size of 1024
                out.write((Integer.toHexString(part.length) + "\r\n").getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                out.write(part);
                out.write("\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                out.flush();
            }
            String statusLine = new java.io.BufferedReader(
                    new java.io.InputStreamReader(socket.getInputStream(), java.nio.charset.StandardCharsets.US_ASCII)).readLine();
            assertTrue(statusLine.startsWith("HTTP/1.1 413"), statusLine);
        }
        assertEquals(1, store().abortCalls.get());
        assertEquals(0, store().findUploadInfo(id).orElseThrow().getOffset());
    }

    /** BufferingUploadStore exists for stores whose append blocks; it must not run on the event loop. */
    @Test
    void bufferingStoreAppendRunsOffTheEventLoop() {
        String location = createUpload(100);
        uploadData(location, new byte[10], 0);
        assertFalse(store().appendRanOnEventLoop.get(), "appendBytes ran on a Vert.x event loop thread");
    }
}
