package org.sitenetsoft.quarkus.tus.client.runtime.source;

import io.smallrye.mutiny.Multi;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileUploadSourceTest {

    private static final String CONTENT = "hello, tus client world!";

    private Vertx vertx;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        var latch = new java.util.concurrent.CountDownLatch(1);
        vertx.close().onComplete(r -> latch.countDown());
        latch.await(10, TimeUnit.SECONDS);
    }

    private byte[] collect(Multi<Buffer> multi) {
        Buffer collected = multi.collect().in(Buffer::buffer, Buffer::appendBuffer)
                .await().atMost(Duration.ofSeconds(10));
        return collected.getBytes();
    }

    @Test
    void lengthReflectsFileSizeAtConstruction(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("data.bin");
        Files.write(file, CONTENT.getBytes(StandardCharsets.UTF_8));

        UploadSource source = UploadSource.ofFile(vertx, file);

        assertEquals(CONTENT.getBytes(StandardCharsets.UTF_8).length, source.length());
    }

    @Test
    void sliceFromZeroYieldsFullContent(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("data.bin");
        Files.write(file, CONTENT.getBytes(StandardCharsets.UTF_8));
        UploadSource source = UploadSource.ofFile(vertx, file);

        byte[] result = collect(source.slice(0));

        assertEquals(CONTENT, new String(result, StandardCharsets.UTF_8));
    }

    @Test
    void sliceFromOffsetYieldsSuffix(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("data.bin");
        Files.write(file, CONTENT.getBytes(StandardCharsets.UTF_8));
        UploadSource source = UploadSource.ofFile(vertx, file);

        byte[] result = collect(source.slice(7));

        assertEquals(CONTENT.substring(7), new String(result, StandardCharsets.UTF_8));
    }

    @Test
    void sliceIsRereadable(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("data.bin");
        Files.write(file, CONTENT.getBytes(StandardCharsets.UTF_8));
        UploadSource source = UploadSource.ofFile(vertx, file);

        byte[] first = collect(source.slice(0));
        byte[] second = collect(source.slice(0));

        assertEquals(CONTENT, new String(first, StandardCharsets.UTF_8));
        assertEquals(CONTENT, new String(second, StandardCharsets.UTF_8));
    }

    @Test
    void fileSourceReportsReplayable(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("data.bin");
        Files.write(file, CONTENT.getBytes(StandardCharsets.UTF_8));
        UploadSource source = UploadSource.ofFile(vertx, file);

        assertTrue(source.replayable());
    }

    @Test
    void oneShotIsNotReplayableAndSecondSliceFails() {
        Multi<Buffer> data = Multi.createFrom().item(Buffer.buffer(CONTENT.getBytes(StandardCharsets.UTF_8)));
        UploadSource source = UploadSource.oneShot(data, CONTENT.length());

        assertFalse(source.replayable());

        byte[] result = collect(source.slice(0));
        assertEquals(CONTENT, new String(result, StandardCharsets.UTF_8));

        assertThrows(IllegalStateException.class, () -> source.slice(0));
    }

    @Test
    void oneShotSliceWithNonZeroOffsetFails() {
        Multi<Buffer> data = Multi.createFrom().item(Buffer.buffer(CONTENT.getBytes(StandardCharsets.UTF_8)));
        UploadSource source = UploadSource.oneShot(data, CONTENT.length());

        assertThrows(IllegalStateException.class, () -> source.slice(7));
    }
}
