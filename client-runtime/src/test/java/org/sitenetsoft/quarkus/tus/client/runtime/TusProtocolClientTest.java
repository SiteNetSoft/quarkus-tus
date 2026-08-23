package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
