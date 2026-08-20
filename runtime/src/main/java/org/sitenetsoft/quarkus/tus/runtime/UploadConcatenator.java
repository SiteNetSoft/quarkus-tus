package org.sitenetsoft.quarkus.tus.runtime;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Turns a pending final upload into a real one by joining its partials, once they are all
 * complete. The framework may reach this from two directions — the POST that creates the final
 * upload, and a later HEAD on it — so the readiness check, the locking and the join live here
 * rather than in either caller.
 */
@ApplicationScoped
public class UploadConcatenator {

    @Inject
    UploadStore uploadStore;

    @Inject
    UploadEvents events;

    /**
     * Joins the partials into {@code finalId} if every one of them is complete, under the final
     * upload's lock and every partial's lock. Resolves to {@code true} if the concatenation
     * happened, {@code false} if it is not ready or another request is doing it; fails only if
     * the store's join failed.
     */
    public Uni<Boolean> finalizeIfReady(String finalId, UploadInfo finalInfo) {
        List<String> partialIds = finalInfo.getPartialIds();
        if (!finalInfo.isFinalConcat() || partialIds == null || partialIds.isEmpty()) {
            return Uni.createFrom().item(false);
        }
        return uploadStore.acquireLock(finalId).chain(gotFinalLock -> {
            if (!gotFinalLock) {
                return Uni.createFrom().item(false);
            }
            List<String> locked = new ArrayList<>();
            return lockCompletePartials(partialIds, locked)
                    // Re-read under the lock: a concurrent HEAD may already have finalized it.
                    .chain(ready -> ready ? uploadStore.findUploadInfo(finalId)
                            : Uni.createFrom().item(Optional.<UploadInfo>empty()))
                    .chain(currentOpt -> {
                        if (currentOpt.isEmpty() || !currentOpt.get().isFinalConcat()) {
                            return Uni.createFrom().item(false);
                        }
                        UploadInfo info = currentOpt.get();
                        return uploadStore.concatenate(finalId, partialIds)
                                // The partials are discarded under the locks this request still
                                // holds, so a second final over the same partials cannot slip in
                                // between and find them half gone.
                                .chain(() -> discardAll(partialIds))
                                .emitOn(Infrastructure.getDefaultWorkerPool())
                                .invoke(() -> events.concatenationCompleted(finalId, partialIds, info))
                                .replaceWith(true);
                    })
                    .eventually(() -> releaseAll(locked).chain(() -> uploadStore.releaseLock(finalId)));
        });
    }

    /**
     * Takes each partial's lock in turn, recording what was taken in {@code locked} so the caller
     * can release it, and stops at the first partial that is locked elsewhere, missing or
     * incomplete. Resolves to whether every partial is locked and complete.
     */
    private Uni<Boolean> lockCompletePartials(List<String> partialIds, List<String> locked) {
        Uni<Boolean> chain = Uni.createFrom().item(true);
        for (String partialId : partialIds) {
            chain = chain.chain(stillReady -> {
                if (!stillReady) {
                    return Uni.createFrom().item(false);
                }
                return uploadStore.acquireLock(partialId).chain(gotLock -> {
                    if (!gotLock) {
                        return Uni.createFrom().item(false);
                    }
                    locked.add(partialId);
                    return uploadStore.findUploadInfo(partialId).map(partial -> partial.isPresent()
                            && partial.get().getOffset() == partial.get().getEntityLength());
                });
            });
        }
        return chain;
    }

    /** Discards every id in turn, clearing its progress entry; the caller holds their locks. */
    private Uni<Void> discardAll(List<String> ids) {
        return Multi.createFrom().iterable(ids)
                .onItem().transformToUniAndConcatenate(id -> uploadStore.discardUpload(id)
                        .invoke(() -> events.uploadDiscarded(id)))
                .collect().last()
                .replaceWithVoid();
    }

    private Uni<Void> releaseAll(List<String> ids) {
        return Multi.createFrom().iterable(List.copyOf(ids))
                .onItem().transformToUniAndConcatenate(uploadStore::releaseLock)
                .collect().last()
                .replaceWithVoid();
    }
}
