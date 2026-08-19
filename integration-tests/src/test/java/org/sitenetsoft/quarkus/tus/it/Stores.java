package org.sitenetsoft.quarkus.tus.it;

import io.smallrye.mutiny.Uni;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.time.Duration;
import java.util.Optional;

/**
 * Awaits {@link UploadStore}'s asynchronous methods, for tests that poke the store directly.
 * A test is not a request: it can block, and everything it asserts on has settled by then.
 */
final class Stores {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    private Stores() {
    }

    static <T> T await(Uni<T> uni) {
        return uni.await().atMost(TIMEOUT);
    }

    static Optional<UploadInfo> find(UploadStore store, String id) {
        return await(store.findUploadInfo(id));
    }

    static String create(UploadStore store, UploadInfo info) {
        return await(store.createUpload(info));
    }

    static void update(UploadStore store, String id, UploadInfo info) {
        await(store.updateUploadInfo(id, info));
    }

    static boolean discard(UploadStore store, String id) {
        return await(store.discardUpload(id));
    }

    static boolean lock(UploadStore store, String id) {
        return await(store.acquireLock(id));
    }

    static void unlock(UploadStore store, String id) {
        await(store.releaseLock(id));
    }
}
