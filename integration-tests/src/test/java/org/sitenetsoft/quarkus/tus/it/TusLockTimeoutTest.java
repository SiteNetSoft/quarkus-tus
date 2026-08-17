package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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

    private String newUpload() {
        UploadInfo info = new UploadInfo();
        info.setEntityLength(1000L);
        info.setOffset(0L);
        return uploadStore.createUpload(info);
    }

    @Test
    void lockIsNotReclaimedWhileAChunkIsStillStreaming() throws Exception {
        String id = newUpload();
        assertTrue(uploadStore.acquireLock(id));
        try {
            // Six buffers, 300 ms apart: the write outlives the 1 s timeout by a wide margin.
            Multi<Buffer> slowBody = Multi.createFrom().ticks().every(Duration.ofMillis(300))
                    .select().first(6)
                    .onItem().transform(t -> Buffer.buffer(new byte[10]));
            CompletableFuture<Long> staged = uploadStore.stageChunk(id, 0, slowBody, -1)
                    .subscribeAsCompletionStage();

            Thread.sleep(1_400);
            assertFalse(uploadStore.acquireLock(id), "lock reclaimed while a write was still streaming");

            assertEquals(60L, staged.get(10, TimeUnit.SECONDS));
            uploadStore.commitChunk(id, 0, 60).await().atMost(Duration.ofSeconds(5));
        } finally {
            uploadStore.releaseLock(id);
            uploadStore.discardUpload(id);
        }
    }

    @Test
    void idleLockIsReclaimedAfterTheConfiguredTimeout() throws Exception {
        String id = newUpload();
        assertTrue(uploadStore.acquireLock(id));
        try {
            Thread.sleep(1_300);
            assertTrue(uploadStore.acquireLock(id), "an abandoned lock must be reclaimable after the timeout");
        } finally {
            uploadStore.releaseLock(id);
            uploadStore.discardUpload(id);
        }
    }
}
