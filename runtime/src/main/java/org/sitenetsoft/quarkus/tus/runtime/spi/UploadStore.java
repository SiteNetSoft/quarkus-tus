package org.sitenetsoft.quarkus.tus.runtime.spi;

import io.smallrye.mutiny.Uni;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * SPI for TUS upload storage. Consumers can provide alternative implementations
 * via {@code @Alternative @Priority(1)}.
 */
public interface UploadStore {

    Optional<UploadInfo> findUploadInfo(String id);

    Optional<String> createUpload(Long totalLength, Optional<String> uploadMetadata, boolean isPartial);

    void setUploaderId(String id, String uploaderId);

    String getUploaderId(String id);

    Optional<String> createUploadDeferred(Optional<String> uploadMetadata, boolean isPartial);

    boolean setDeferredLength(String id, long length);

    boolean hasDeferredLength(String id);

    /**
     * Merges complete partial uploads into a final one.
     * <p>
     * {@code uploadConcatHeader} is the raw {@code Upload-Concat} request header. It must be
     * stored verbatim rather than rebuilt from {@code ids}, because the protocol requires HEAD
     * on a final upload to return the value as the client sent it.
     */
    Optional<String> mergePartialUploadsWithOwnership(String[] ids, Optional<String> uploadMetadata,
                                                      String requiredOwnerId, String uploadConcatHeader);

    /**
     * Creates a final concatenation whose partials are not all complete yet.
     * Implementations must apply the same ownership validation as
     * {@link #mergePartialUploadsWithOwnership}: a non-null {@code requiredOwnerId}
     * must match the uploader of every referenced partial that has one.
     */
    Optional<String> mergePartialUploadsUnfinished(String[] ids, Optional<String> uploadMetadata,
                                                   String requiredOwnerId, String uploadConcatHeader);

    boolean isConcatReady(String id);

    boolean finalizeConcatenation(String id);

    boolean checkServerSizeConstraint(Long totalLength);

    boolean discardUpload(String id);

    boolean acquireLock(String id);

    void releaseLock(String id);

    Uni<Long> writeChunkAsync(String id, long offset, byte[] chunk, Optional<UploadInfo.ChecksumInfo> checksum);

    boolean validateOffset(String id, long clientOffset);

    long writeInitialData(String id, byte[] data);

    boolean isExpired(String id);

    Optional<Instant> getExpiresAt(String id);

    List<String> cleanupExpiredUploads();

    /**
     * Releases locks whose holder died. Called by the scheduler roughly once a minute.
     * <p>
     * This and the two hooks below default to doing nothing so that a store with no such concept
     * need not implement them — but a store that does hold locks or write files must, or its
     * maintenance simply never runs.
     */
    default void cleanupStaleLocks() {
    }

    /**
     * Removes incomplete uploads with no activity for {@code staleHours}.
     *
     * @return the uploads actually removed, which is what the scheduler logs
     */
    default List<String> cleanupStaleUploads(long staleHours) {
        return List.of();
    }

    /**
     * Removes stored data with no corresponding upload, e.g. left by a crash.
     *
     * @return how many were removed
     */
    default int cleanupOrphanFiles() {
        return 0;
    }
}
