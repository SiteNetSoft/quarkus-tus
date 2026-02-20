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

    Optional<String> mergePartialUploads(String[] ids, Optional<String> uploadMetadata);

    Optional<String> mergePartialUploadsWithOwnership(String[] ids, Optional<String> uploadMetadata, String requiredOwnerId);

    Optional<String> mergePartialUploadsUnfinished(String[] ids, Optional<String> uploadMetadata);

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
}
