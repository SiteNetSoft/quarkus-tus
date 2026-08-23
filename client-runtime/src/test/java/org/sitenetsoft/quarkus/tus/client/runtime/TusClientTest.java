package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadProgress;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TusClientTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private Vertx vertx;
    private ScriptedTusServer server;
    private TusClient client;

    @BeforeEach
    void setUp() throws InterruptedException {
        vertx = Vertx.vertx();
        server = new ScriptedTusServer(vertx);
        server.start();
        client = TusClient.create(vertx, TusClientOptions.builder(server.url()).build());
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.close();
        vertx.close();
    }

    private void enqueueOptions(String... extensions) {
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of(
                "Tus-Resumable", "1.0.0",
                "Tus-Version", "1.0.0",
                "Tus-Extension", String.join(",", extensions))));
    }

    /**
     * A trivial in-memory, fully replayable {@link UploadSource}: {@code slice(offset)} is called
     * once per chunk by the upload loop, so a one-shot source (consumable only once, from offset
     * zero) can't stand in here the way it can for a single creation-with-upload body.
     */
    private static UploadSource sourceOf(String text) {
        Buffer data = Buffer.buffer(text, StandardCharsets.UTF_8.name());
        return new UploadSource() {
            @Override
            public long length() {
                return data.length();
            }

            @Override
            public Multi<Buffer> slice(long fromOffset) {
                return Multi.createFrom().item(data.getBuffer((int) fromOffset, data.length()));
            }
        };
    }

    @Test
    void uploadsInChunksAndReportsProgress() {
        // 11 bytes, chunk 4 -> create, then PATCH offsets 0,4,8
        enqueueOptions("creation");
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4")));
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "8")));
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
        var progress = new ArrayList<TusUploadProgress>();
        var result = client.upload(TusUploadRequest.builder(sourceOf("hello world"))
                .chunkSize(4).onProgress(progress::add).build()).await().atMost(TIMEOUT);
        assertEquals(11, result.bytesUploaded());
        assertEquals(List.of("OPTIONS", "POST", "PATCH", "PATCH", "PATCH"),
                server.recorded().stream().map(ScriptedTusServer.Recorded::method).toList());
        assertEquals("8", server.recorded().get(4).headers().get("Upload-Offset"));
        assertEquals(11L, progress.getLast().bytesSent());
    }

    @Test
    void bufferLimiterTruncatesTheFinalBufferAcrossBufferBoundaries() {
        Multi<Buffer> upstream = Multi.createFrom().items(
                Buffer.buffer("ab"), Buffer.buffer("cde"), Buffer.buffer("fghij"));
        List<Buffer> emitted = BufferLimiter.limit(upstream, 6).collect().asList().await().atMost(TIMEOUT);
        Buffer joined = Buffer.buffer();
        emitted.forEach(joined::appendBuffer);
        assertEquals("abcdef", joined.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    void bufferLimiterEmitsEverythingWhenTheLimitExceedsTheUpstream() {
        Multi<Buffer> upstream = Multi.createFrom().items(Buffer.buffer("ab"), Buffer.buffer("cd"));
        List<Buffer> emitted = BufferLimiter.limit(upstream, 100).collect().asList().await().atMost(TIMEOUT);
        Buffer joined = Buffer.buffer();
        emitted.forEach(joined::appendBuffer);
        assertEquals("abcd", joined.toString(StandardCharsets.UTF_8.name()));
    }
}
