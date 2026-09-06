package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.vertx.core.buffer.Buffer;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.OffsetMismatchException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The upload lock is held for the whole body transfer now, not just a local disk write, so a
 * slow client's lock must not be reclaimed as stale while its bytes are still arriving. The
 * timeout is set to one second here so the test can run in a few.
 */
@QuarkusTest
@TestProfile(TusLockTimeoutTest.ShortLockTimeoutProfile.class)
class TusLockTimeoutTest {

    public static class ShortLockTimeoutProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.tus.lock-timeout-seconds", "1");
        }
    }

    @Inject
    UploadStore uploadStore;

    @ConfigProperty(name = "quarkus.tus.store.local.upload-dir")
    String uploadDir;

    private String newUpload() {
        UploadInfo info = new UploadInfo();
        info.setEntityLength(1000L);
        info.setOffset(0L);
        return Stores.create(uploadStore, info);
    }

    @Test
    void lockIsNotReclaimedWhileAChunkIsStillStreaming() throws Exception {
        String id = newUpload();
        assertTrue(Stores.lock(uploadStore, id));
        try {
            // Six buffers, 300 ms apart: the write outlives the 1 s timeout by a wide margin.
            Multi<Buffer> slowBody = Multi.createFrom().ticks().every(Duration.ofMillis(300))
                    .select().first(6)
                    .onItem().transform(t -> Buffer.buffer(new byte[10]));
            CompletableFuture<Long> staged = uploadStore.stageChunk(id, 0, slowBody, -1)
                    .subscribeAsCompletionStage();

            Thread.sleep(1_400);
            assertFalse(Stores.lock(uploadStore, id), "lock reclaimed while a write was still streaming");

            assertEquals(60L, staged.get(10, TimeUnit.SECONDS));
            uploadStore.commitChunk(id, 0, 60).await().atMost(Duration.ofSeconds(5));
        } finally {
            Stores.unlock(uploadStore, id);
            Stores.discard(uploadStore, id);
        }
    }

    @Test
    void idleLockIsReclaimedAfterTheConfiguredTimeout() throws Exception {
        String id = newUpload();
        assertTrue(Stores.lock(uploadStore, id));
        try {
            Thread.sleep(1_300);
            assertTrue(Stores.lock(uploadStore, id), "an abandoned lock must be reclaimable after the timeout");
        } finally {
            Stores.unlock(uploadStore, id);
            Stores.discard(uploadStore, id);
        }
    }

    /**
     * Reclaiming a stale lock must fence the holder it was taken from. Request A stalls
     * mid-chunk with its socket still open; B reclaims the lock, stages and commits its bytes.
     * When A's stream finally dies, everything A does next — the store's own truncation on
     * stage failure, the framework's abortChunk, its releaseLock — belongs to a lock that no
     * longer exists, and must touch neither B's bytes nor B's lock. Before the fence, A's abort
     * cut the file back to A's offset while the record said B's, leaving a zero-filled hole.
     */
    @Test
    void reclaimedLockFencesTheStalledHolder() throws Exception {
        String id = newUpload();
        Path file = Path.of(uploadDir, id);
        assertTrue(Stores.lock(uploadStore, id), "A takes the lock");
        AtomicReference<MultiEmitter<? super Buffer>> stalled = new AtomicReference<>();
        try {
            Multi<Buffer> aBody = Multi.createFrom().<Buffer>emitter(e -> stalled.set(e));
            CompletableFuture<Long> aStage = uploadStore.stageChunk(id, 0, aBody, -1)
                    .subscribeAsCompletionStage();
            awaitEmitter(stalled);
            stalled.get().emit(Buffer.buffer(new byte[10])); // A lands ten bytes, then stalls

            Thread.sleep(1_300);
            // The scheduled sweep runs first, as it may in production: it must leave a lock
            // whose holder is still streaming for on-demand reclamation, or A's eventual
            // release would free whatever lock B holds by then.
            Stores.await(uploadStore.cleanupStaleLocks());
            assertTrue(Stores.lock(uploadStore, id), "B reclaims the abandoned lock");

            long bStaged = Stores.await(uploadStore.stageChunk(id, 0,
                    Multi.createFrom().item(Buffer.buffer(new byte[20])), 20));
            assertEquals(20, bStaged);
            Stores.await(uploadStore.commitChunk(id, 0, 20));
            assertEquals(20, Files.size(file));
            assertEquals(20, Stores.find(uploadStore, id).orElseThrow().getOffset());

            // A wakes up: its next buffer must not be written into B's upload ...
            stalled.get().emit(Buffer.buffer(new byte[30]));
            stalled.get().complete();
            ExecutionException failed = assertThrows(ExecutionException.class,
                    () -> aStage.get(10, TimeUnit.SECONDS));
            assertInstanceOf(OffsetMismatchException.class, failed.getCause(),
                    "a displaced stage must fail rather than write, got " + failed.getCause());
            assertEquals(20, Files.size(file), "A's late bytes leaked into the file");

            // ... and the framework's abort and release on A's behalf must be fenced too.
            assertThrows(Exception.class, () -> Stores.await(uploadStore.abortChunk(id, 0)),
                    "a stale abort must be refused, not roll B's commit back");
            Stores.unlock(uploadStore, id); // A releasing: must not drop B's lock

            assertEquals(20, Files.size(file), "file was truncated below the committed offset");
            assertEquals(20, Stores.find(uploadStore, id).orElseThrow().getOffset());
            assertFalse(Stores.lock(uploadStore, id), "B's lock was removed by A's release");
        } finally {
            Stores.unlock(uploadStore, id);
            Stores.discard(uploadStore, id);
        }
    }

    private static void awaitEmitter(AtomicReference<MultiEmitter<? super Buffer>> ref) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000;
        while (ref.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertNotNull(ref.get(), "the store never subscribed to the body");
    }
}
