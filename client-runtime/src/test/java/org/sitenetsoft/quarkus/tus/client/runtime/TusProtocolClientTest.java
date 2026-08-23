package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusOffsetMismatchException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusProtocolException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TusProtocolClientTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private Vertx vertx;
    private ScriptedTusServer server;
    private TusProtocolClient client;

    @BeforeEach
    void setUp() throws InterruptedException {
        vertx = Vertx.vertx();
        server = new ScriptedTusServer(vertx);
        server.start();
        client = new TusProtocolClient(vertx, TusTarget.builder(server.url()).build());
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.close();
        vertx.close();
    }

    @Test
    void optionsParsesCapabilities() {
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of(
                "Tus-Resumable", "1.0.0", "Tus-Version", "1.0.0",
                "Tus-Extension", "creation,termination,checksum", "Tus-Max-Size", "1073741824",
                "Tus-Checksum-Algorithm", "sha1,md5")));
        var caps = client.options().await().atMost(TIMEOUT);
        assertTrue(caps.supports("checksum"));
        assertEquals(OptionalLong.of(1073741824L), caps.maxSize());
        assertEquals(Set.of("sha1", "md5"), caps.checksumAlgorithms());
        var sent = server.recorded().getFirst();
        assertEquals("OPTIONS", sent.method());
        assertEquals("1.0.0", sent.headers().get("Tus-Resumable"));
    }

    @Test
    void createSendsLengthAndMetadataAndResolvesRelativeLocation() {
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/abc123")));
        var upload = client.create(TusCreateOptions.builder().length(42)
                .metadata(Map.of("filename", "cat.png")).build()).await().atMost(TIMEOUT);
        assertEquals(server.url() + "/abc123", upload.url());
        assertEquals(0, upload.offset());
        assertEquals(42, upload.length());
        var sent = server.recorded().getFirst();
        assertEquals("POST", sent.method());
        assertEquals("42", sent.headers().get("Upload-Length"));
        assertEquals("filename Y2F0LnBuZw==", sent.headers().get("Upload-Metadata"));
    }

    @Test
    void createWithUploadStreamsTheBodyAndReturnsTheOffset() {
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0",
                "Location", "/tus/abc", "Upload-Offset", "5")));
        var upload = client.create(TusCreateOptions.builder().length(10)
                .body(Multi.createFrom().item(Buffer.buffer("hello")), 5).build()).await().atMost(TIMEOUT);
        assertEquals(5, upload.offset());
        var sent = server.recorded().getFirst();
        assertEquals("application/offset+octet-stream", sent.headers().get("Content-Type"));
        assertEquals("hello", sent.body().toString());
    }

    @Test
    void deferLengthSendsTheDeferHeader() {
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/x")));
        client.create(TusCreateOptions.builder().deferLength().build()).await().atMost(TIMEOUT);
        assertEquals("1", server.recorded().getFirst().headers().get("Upload-Defer-Length"));
    }

    @Test
    void partialCreateSendsUploadConcatPartial() {
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/p1")));
        client.create(TusCreateOptions.builder().length(5).partial().build()).await().atMost(TIMEOUT);
        assertEquals("partial", server.recorded().getFirst().headers().get("Upload-Concat"));
    }

    @Test
    void malformedUploadExpiresFailsAsTusProtocolException() {
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0",
                "Location", "/tus/x", "Upload-Expires", "not-a-valid-date")));
        var uni = client.create(TusCreateOptions.builder().length(1).build());
        assertThrows(TusProtocolException.class, () -> uni.await().atMost(TIMEOUT));
    }

    @Test
    void offsetReadsUploadOffsetFromHead() {
        server.enqueue(ScriptedTusServer.Canned.of(200, Map.of("Tus-Resumable", "1.0.0",
                "Upload-Offset", "17", "Cache-Control", "no-store")));
        assertEquals(17L, client.offset(server.url() + "/abc").await().atMost(TIMEOUT));
        assertEquals("HEAD", server.recorded().getFirst().method());
    }

    @Test
    void patchStreamsAtOffsetAndReturnsTheNewOffset() {
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "10")));
        long newOffset = client.patch(server.url() + "/abc", 5,
                Multi.createFrom().item(Buffer.buffer("hello")),
                TusPatchOptions.builder().contentLength(5).build()).await().atMost(TIMEOUT);
        assertEquals(10L, newOffset);
        var sent = server.recorded().getFirst();
        assertEquals("PATCH", sent.method());
        assertEquals("5", sent.headers().get("Upload-Offset"));
        assertEquals("application/offset+octet-stream", sent.headers().get("Content-Type"));
        assertEquals("5", sent.headers().get("Content-Length"));
        assertEquals("hello", sent.body().toString());
    }

    @Test
    void patchCanCarryChecksumAndDeclaredLength() {
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "5")));
        client.patch(server.url() + "/abc", 0, Multi.createFrom().item(Buffer.buffer("hello")),
                TusPatchOptions.builder().contentLength(5)
                        .checksum("sha1", "qvTGHdzF6KLavt4PO0gs2a6pQ00=")
                        .declareUploadLength(5).build()).await().atMost(TIMEOUT);
        var sent = server.recorded().getFirst();
        assertEquals("sha1 qvTGHdzF6KLavt4PO0gs2a6pQ00=", sent.headers().get("Upload-Checksum"));
        assertEquals("5", sent.headers().get("Upload-Length"));
    }

    @Test
    void patchOffsetMismatchIsTyped() {
        server.enqueue(ScriptedTusServer.Canned.of(409, Map.of("Tus-Resumable", "1.0.0")));
        assertThrows(TusOffsetMismatchException.class, () ->
                client.patch(server.url() + "/abc", 3, Multi.createFrom().item(Buffer.buffer("x")),
                        TusPatchOptions.none()).await().atMost(TIMEOUT));
    }

    @Test
    void terminateSendsDelete() {
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0")));
        client.terminate(server.url() + "/abc").await().atMost(TIMEOUT);
        assertEquals("DELETE", server.recorded().getFirst().method());
    }

    @Test
    void concatenateJoinsPartialUrlsSpaceSeparated() {
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/final1")));
        var fin = client.concatenate(List.of(server.url() + "/p1", server.url() + "/p2"),
                TusCreateOptions.builder().build()).await().atMost(TIMEOUT);
        var sent = server.recorded().getFirst();
        assertEquals("final;" + server.url() + "/p1 " + server.url() + "/p2",
                sent.headers().get("Upload-Concat"));
        assertNull(sent.headers().get("Upload-Length")); // spec: final creation has no Upload-Length
    }

    /** Every row of the mapping table, exercised through a real HTTP exchange. */
    @ParameterizedTest
    @CsvSource({"404,TusUploadNotFoundException", "410,TusUploadNotFoundException",
            "412,TusVersionMismatchException", "413,TusPayloadTooLargeException",
            "460,TusChecksumMismatchException", "500,TusServerErrorException",
            "503,TusServerErrorException"})
    void errorStatusesAreTyped(int status, String exceptionSimpleName) {
        server.enqueue(ScriptedTusServer.Canned.of(status, Map.of("Tus-Resumable", "1.0.0")));
        var failure = assertThrows(TusClientException.class, () ->
                client.offset(server.url() + "/abc").await().atMost(TIMEOUT));
        assertEquals(exceptionSimpleName, failure.getClass().getSimpleName());
    }
}
