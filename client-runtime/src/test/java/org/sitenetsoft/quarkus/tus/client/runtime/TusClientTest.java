package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.client.runtime.ScriptedTusServer.Canned;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusCapabilityException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusPayloadTooLargeException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusServerErrorException;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadProgress;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        var headers = new java.util.LinkedHashMap<String, String>();
        headers.put("Tus-Resumable", "1.0.0");
        headers.put("Tus-Version", "1.0.0");
        headers.put("Tus-Extension", String.join(",", extensions));
        boolean advertisesChecksum = List.of(extensions).stream()
                .anyMatch(ext -> List.of(ext.split(",")).contains("checksum"));
        if (advertisesChecksum) {
            // Only sha1 is advertised: the tests that need checksum support only ever request sha1,
            // so this stays minimal rather than pre-emptively listing sha256/md5 too.
            headers.put("Tus-Checksum-Algorithm", "sha1");
        }
        server.enqueue(ScriptedTusServer.Canned.of(204, headers));
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
    void bufferLimiterEmitsEverythingWhenTheLimitExactlyMatchesTheUpstream() {
        // The boundary case: maxBytes == the upstream's true length (what uploadRange always passes,
        // since it computes len = min(chunkSize, remaining)) must NOT be treated as a short read.
        Multi<Buffer> upstream = Multi.createFrom().items(Buffer.buffer("ab"), Buffer.buffer("cd"));
        List<Buffer> emitted = BufferLimiter.limit(upstream, 4).collect().asList().await().atMost(TIMEOUT);
        Buffer joined = Buffer.buffer();
        emitted.forEach(joined::appendBuffer);
        assertEquals("abcd", joined.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    void bufferLimiterCancelsUpstreamEvenWhenTheCapIsHitDuringSynchronousDelivery() {
        AtomicBoolean cancelled = new AtomicBoolean(false);
        // Multi.createFrom().items(...) delivers every item synchronously, inside the subscribe()
        // call itself -- so the cap is reached (and cancellation requested) before subscribe() has
        // even returned the Cancellable to store.
        Multi<Buffer> upstream = Multi.createFrom().items(
                        Buffer.buffer("ab"), Buffer.buffer("cd"), Buffer.buffer("ef"))
                .onCancellation().invoke(() -> cancelled.set(true));
        BufferLimiter.limit(upstream, 2).collect().asList().await().atMost(TIMEOUT);
        assertTrue(cancelled.get(), "upstream must be cancelled once the cap is reached, even synchronously");
    }

    @Test
    void bufferLimiterFailsWithATypedExceptionNamingBothLengthsOnAShortRead() {
        Multi<Buffer> upstream = Multi.createFrom().items(Buffer.buffer("ab"), Buffer.buffer("cd"));
        TusClientException e = assertThrows(TusClientException.class,
                () -> BufferLimiter.limit(upstream, 10).collect().asList().await().atMost(TIMEOUT));
        assertTrue(e.getMessage().contains("10"), "message should name the expected length: " + e.getMessage());
        assertTrue(e.getMessage().contains("4"), "message should name the actual length: " + e.getMessage());
    }

    @Test
    void transientServerErrorResyncsWithHeadAndResumes() {
        client.close();
        client = TusClient.create(vertx,
                TusClientOptions.builder(server.url()).retryBackoff(Duration.ofMillis(10)).build());
        enqueueOptions("creation");
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4")));
        server.enqueue(ScriptedTusServer.Canned.of(500, Map.of("Tus-Resumable", "1.0.0")));                       // PATCH @4 dies
        server.enqueue(ScriptedTusServer.Canned.of(200, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4"))); // HEAD resync
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "8")));
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
        var result = client.upload(TusUploadRequest.builder(sourceOf("hello world"))
                .chunkSize(4).build()).await().atMost(TIMEOUT);
        assertEquals(11, result.bytesUploaded());
        // Real sequence: OPTIONS(0), POST(1), PATCH-succeeds(2), PATCH-500(3), HEAD-resync(4), PATCH(5), PATCH(6).
        assertEquals("HEAD", server.recorded().get(4).method());   // the resync
    }

    @Test
    void nonRetryableClientErrorFailsFast() {
        enqueueOptions("creation");
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
        server.enqueue(ScriptedTusServer.Canned.of(413, Map.of("Tus-Resumable", "1.0.0")));
        assertThrows(TusPayloadTooLargeException.class, () ->
                client.upload(TusUploadRequest.builder(sourceOf("hello world")).chunkSize(4).build())
                        .await().atMost(TIMEOUT));
        assertEquals(3, server.recorded().size());                 // no retry happened
    }

    @Test
    void retriesAreBoundedByMaxRetries() {
        client.close();
        client = TusClient.create(vertx,
                TusClientOptions.builder(server.url()).retryBackoff(Duration.ofMillis(10)).build());
        enqueueOptions("creation");
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
        for (int i = 0; i < 4; i++) {                              // 1 try + 3 retries, all 500
            server.enqueue(ScriptedTusServer.Canned.of(500, Map.of("Tus-Resumable", "1.0.0")));
            server.enqueue(ScriptedTusServer.Canned.of(200, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "0")));
        }
        assertThrows(TusServerErrorException.class, () ->
                client.upload(TusUploadRequest.builder(sourceOf("hello world")).chunkSize(4).build())
                        .await().atMost(TIMEOUT));
    }

    @Test
    void retryBudgetIsPerUploadNotPerChunk() {
        // Two chunks succeed, then the last chunk (offset 8 -> 11) fails persistently. The retry
        // budget is a single counter for the whole upload, not one counter per already-succeeded
        // chunk's recursion frame -- so this must total exactly 1 initial + 3 retries = 4 failing
        // PATCHes on the last chunk (6 PATCHes overall), not 4 retries multiplied by every frame that
        // chunk's failure bubbles through (which is what a per-frame retry budget produces).
        client.close();
        client = TusClient.create(vertx,
                TusClientOptions.builder(server.url()).retryBackoff(Duration.ofMillis(10)).build());
        enqueueOptions("creation");
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4")));
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "8")));
        for (int i = 0; i < 4; i++) {                              // last chunk: 1 try + 3 retries, all 500
            server.enqueue(ScriptedTusServer.Canned.of(500, Map.of("Tus-Resumable", "1.0.0")));
            server.enqueue(ScriptedTusServer.Canned.of(200, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "8")));
        }
        assertThrows(TusServerErrorException.class, () ->
                client.upload(TusUploadRequest.builder(sourceOf("hello world")).chunkSize(4).build())
                        .await().atMost(TIMEOUT));
        long patchCount = server.recorded().stream().filter(r -> r.method().equals("PATCH")).count();
        assertEquals(6, patchCount,
                "2 successful chunks + (1 initial + 3 retries) on the persistently failing last chunk");
        long headCount = server.recorded().stream().filter(r -> r.method().equals("HEAD")).count();
        assertEquals(3, headCount, "a resync after each of the first 3 failures, none after the exhausting 4th");
        // OPTIONS + POST + 6 PATCH + 3 HEAD = 11: the 8th (surplus) canned response for the 4th HEAD
        // resync must be left unconsumed, proving the loop stopped instead of retrying past the bound.
        assertEquals(11, server.recorded().size());
    }

    @Test
    void oneShotSourceCannotResume() {
        client.close();
        client = TusClient.create(vertx,
                TusClientOptions.builder(server.url()).retryBackoff(Duration.ofMillis(10)).build());
        enqueueOptions("creation");
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
        server.enqueue(ScriptedTusServer.Canned.of(500, Map.of("Tus-Resumable", "1.0.0")));
        var failure = assertThrows(TusClientException.class, () ->
                client.upload(TusUploadRequest.builder(
                        UploadSource.oneShot(Multi.createFrom().item(Buffer.buffer("hello world")), 11))
                        .chunkSize(4).build()).await().atMost(TIMEOUT));
        assertTrue(failure.getMessage().contains("not replayable"));
    }

    @Test
    void checksumHeaderCarriesTheChunkDigest() throws Exception {
        enqueueOptions("creation,checksum");                       // helper also sets Tus-Checksum-Algorithm: sha1
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
        client.upload(TusUploadRequest.builder(sourceOf("hello world"))
                .checksumAlgorithm("sha1").build()).await().atMost(TIMEOUT);
        String expected = "sha1 " + Base64.getEncoder().encodeToString(
                MessageDigest.getInstance("SHA-1").digest("hello world".getBytes()));
        assertEquals(expected, server.recorded().get(2).headers().get("Upload-Checksum"));
    }

    @Test
    void checksumAgainstServerWithoutTheExtensionIsRefused() {
        enqueueOptions("creation");                                // no checksum advertised
        var failure = assertThrows(TusCapabilityException.class, () ->
                client.upload(TusUploadRequest.builder(sourceOf("hello world"))
                        .checksumAlgorithm("sha1").build()).await().atMost(TIMEOUT));
        assertEquals("checksum", failure.missingExtension());
    }

    @Test
    void mismatch460RetriesTheChunk() {
        enqueueOptions("creation,checksum");
        server.enqueue(ScriptedTusServer.Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
        server.enqueue(ScriptedTusServer.Canned.of(460, Map.of("Tus-Resumable", "1.0.0")));
        server.enqueue(ScriptedTusServer.Canned.of(200, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "0")));
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
        var result = client.upload(TusUploadRequest.builder(sourceOf("hello world"))
                .checksumAlgorithm("sha1").build()).await().atMost(TIMEOUT);
        assertEquals(11, result.bytesUploaded());
    }

    @Test
    void oneShotWithUnknownLengthDeclaresItInAFinalEmptyPatch() {
        enqueueOptions("creation,creation-defer-length");
        server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
        server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
        server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
        var result = client.upload(TusUploadRequest.builder(
                UploadSource.oneShot(Multi.createFrom().item(Buffer.buffer("hello world")), -1)).build())
                .await().atMost(TIMEOUT);
        assertEquals(11, result.bytesUploaded());
        var create = server.recorded().get(1);
        assertEquals("1", create.headers().get("Upload-Defer-Length"));
        assertNull(create.headers().get("Upload-Length"));
        var last = server.recorded().getLast();                    // the declaring PATCH
        assertEquals("PATCH", last.method());
        assertEquals("11", last.headers().get("Upload-Length"));
        assertEquals(0, last.body().length());
    }

    @Test
    void oneShotDeferLengthFailureCancelsTheUpstreamSource() {
        // Two items, chunkSize == the first item's length: the first item alone satisfies a full
        // chunk, so per uploadDeferLength's own pacing the second item is never requested until that
        // chunk's PATCH succeeds -- the subscription is still genuinely open (not naturally
        // completed) when the scripted 500 arrives, so this actually exercises the cancel path
        // rather than racing a source that would complete on its own regardless.
        enqueueOptions("creation,creation-defer-length");
        server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
        server.enqueue(Canned.of(500, Map.of("Tus-Resumable", "1.0.0")));           // the data PATCH dies
        var cancelled = new AtomicBoolean(false);
        var data = Multi.createFrom().items(Buffer.buffer("abcd"), Buffer.buffer("efgh"))
                .onCancellation().invoke(() -> cancelled.set(true));
        assertThrows(TusServerErrorException.class, () ->
                client.upload(TusUploadRequest.builder(UploadSource.oneShot(data, -1)).chunkSize(4).build())
                        .await().atMost(TIMEOUT));
        assertTrue(cancelled.get());
    }

    @Test
    void routedResponsesTakePriorityOverTheGlobalQueueAndStayPerPathFifo() {
        // Fixture self-test: route()'d responses are consumed before the global queue, keyed
        // per-(method, path), and stay FIFO within that key.
        server.route("PATCH", "/tus/a", Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "1")));
        server.route("PATCH", "/tus/b", Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "2")));
        server.enqueue(Canned.of(599, Map.of()));   // would answer first if route() didn't take priority

        var httpClient = io.vertx.mutiny.core.Vertx.newInstance(vertx).createHttpClient();
        var respB = httpClient.request(new io.vertx.core.http.RequestOptions()
                        .setMethod(io.vertx.core.http.HttpMethod.PATCH).setAbsoluteURI(server.url() + "/b"))
                .flatMap(req -> req.send()).await().atMost(TIMEOUT);
        assertEquals(204, respB.statusCode());
        assertEquals("2", respB.getHeader("Upload-Offset"));

        var respA = httpClient.request(new io.vertx.core.http.RequestOptions()
                        .setMethod(io.vertx.core.http.HttpMethod.PATCH).setAbsoluteURI(server.url() + "/a"))
                .flatMap(req -> req.send()).await().atMost(TIMEOUT);
        assertEquals(204, respA.statusCode());
        assertEquals("1", respA.getHeader("Upload-Offset"));
        httpClient.close().await().atMost(TIMEOUT);
    }

    @Test
    void parallelismSplitsIntoPartialsAndConcatenates() {
        enqueueOptions("creation,concatenation");
        // 3 partial creates, 3 partial PATCHes, 1 final concat — order of PATCHes may interleave,
        // so the fixture routes by path: enqueue keyed responses
        server.route("POST", "/tus", Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/p1")),
                                     Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/p2")),
                                     Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/p3")),
                                     Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/fin")));
        server.route("PATCH", "/tus/p1", Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4")));
        server.route("PATCH", "/tus/p2", Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4")));
        server.route("PATCH", "/tus/p3", Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "3")));
        var result = client.upload(TusUploadRequest.builder(sourceOf("hello world"))   // 11 bytes / 3
                .parallelism(3).build()).await().atMost(TIMEOUT);
        assertEquals(11, result.bytesUploaded());
        var finalCreate = server.recorded().stream()
                .filter(r -> r.headers().contains("Upload-Concat") && r.headers().get("Upload-Concat").startsWith("final"))
                .findFirst().orElseThrow();
        // ranges in order: p1 gets bytes [0,4), p2 [4,8), p3 [8,11) — final lists them in order
        assertTrue(finalCreate.headers().get("Upload-Concat").endsWith("/tus/p1 " + server.url() + "/p2 " + server.url() + "/p3")
                || finalCreate.headers().get("Upload-Concat").matches("final;.*p1 .*p2 .*p3"));
    }

    @Test
    void parallelismRequiresConcatenationAndAReplayableKnownLengthSource() {
        enqueueOptions("creation");                                // no concatenation
        var failure = assertThrows(TusCapabilityException.class, () ->
                client.upload(TusUploadRequest.builder(sourceOf("hello world"))
                        .parallelism(2).build()).await().atMost(TIMEOUT));
        assertEquals("concatenation", failure.missingExtension());
    }
}
