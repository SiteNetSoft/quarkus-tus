package org.sitenetsoft.quarkus.tus.runtime.store;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.file.OpenOptions;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.UploadProgressService;
import org.sitenetsoft.quarkus.tus.runtime.config.TusBuildTimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.event.TusUploadCompletedEvent;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class LocalFileUploadStore implements UploadStore {

    private static final Logger LOG = Logger.getLogger(LocalFileUploadStore.class);

    private static final long LOCK_TIMEOUT_MS = 30_000;

    private final Map<String, UploadInfo> uploads = new ConcurrentHashMap<>();
    private final Map<String, Long> activeLocks = new ConcurrentHashMap<>();
    private final AtomicBoolean initValidated = new AtomicBoolean(false);

    private Path uploadBaseDir;

    @Inject
    Vertx vertx;

    @Inject
    UploadProgressService uploadProgressService;

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    @Inject
    TusBuildTimeConfig tusBuildTimeConfig;

    @Inject
    Event<TusUploadCompletedEvent> uploadCompletedEvent;

    @PostConstruct
    void init() {
        this.uploadBaseDir = Path.of(tusRuntimeConfig.store().local().uploadDir()).normalize();
        try {
            Files.createDirectories(uploadBaseDir);
            LOG.infof("TUS uploads dir: %s", uploadBaseDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create uploads directory " + uploadBaseDir, e);
        }

        if (!Files.isWritable(uploadBaseDir)) {
            throw new RuntimeException("TUS uploads directory is not writable: " + uploadBaseDir);
        }

        if (tusRuntimeConfig.maxChunkSize() > tusRuntimeConfig.maxSize()) {
            throw new RuntimeException("quarkus.tus.max-chunk-size (" + tusRuntimeConfig.maxChunkSize()
                    + ") must not exceed quarkus.tus.max-size (" + tusRuntimeConfig.maxSize() + ")");
        }

        String[] algorithms = tusRuntimeConfig.checksumAlgorithms().split(",");
        Set<String> supported = Set.of("sha1", "md5", "sha256");
        for (String alg : algorithms) {
            String trimmed = alg.trim().toLowerCase();
            if (!trimmed.isEmpty() && !supported.contains(trimmed)) {
                LOG.warnf("Unsupported checksum algorithm configured: '%s' (supported: %s)", trimmed, supported);
            }
        }

        initValidated.set(true);
    }

    private Path safePath(String id) {
        Path resolved = uploadBaseDir.resolve(id).normalize();
        if (!resolved.startsWith(uploadBaseDir)) {
            throw new SecurityException("Path traversal attempt detected for id: " + id);
        }
        return resolved;
    }

    @Override
    public Optional<UploadInfo> findUploadInfo(String id) {
        return Optional.ofNullable(uploads.get(id));
    }

    @Override
    public Optional<String> createUpload(Long totalLength, Optional<String> uploadMetadata, boolean isPartial) {
        if (totalLength == null || totalLength < 0) {
            return Optional.empty();
        }

        String id = UUID.randomUUID().toString();

        UploadInfo info = new UploadInfo();
        info.setEntityLength(totalLength);
        info.setOffset(0L);
        info.setPartial(isPartial);
        uploadMetadata.ifPresent(info::setMetadata);

        Instant expiresAt = Instant.now().plus(tusRuntimeConfig.expirationHours(), ChronoUnit.HOURS);
        info.setExpiresAt(expiresAt);

        uploads.put(id, info);
        uploadProgressService.startUpload(id, totalLength);

        Path file = safePath(id);
        try {
            if (!Files.exists(file)) {
                Files.createFile(file);
            }
        } catch (IOException e) {
            LOG.errorf(e, "Failed to create upload file for %s", id);
            uploads.remove(id);
            uploadProgressService.finishUpload(id);
            return Optional.empty();
        }

        return Optional.of(tusBuildTimeConfig.path() + "/" + id);
    }

    @Override
    public void setUploaderId(String id, String uploaderId) {
        UploadInfo info = uploads.get(id);
        if (info != null) {
            info.setUploaderId(uploaderId);
        }
    }

    @Override
    public String getUploaderId(String id) {
        UploadInfo info = uploads.get(id);
        return info != null ? info.getUploaderId() : null;
    }

    @Override
    public Optional<String> createUploadDeferred(Optional<String> uploadMetadata, boolean isPartial) {
        String id = UUID.randomUUID().toString();

        UploadInfo info = new UploadInfo();
        info.setEntityLength(-1);
        info.setOffset(0L);
        info.setPartial(isPartial);
        info.setDeferredLength(true);
        uploadMetadata.ifPresent(info::setMetadata);

        Instant expiresAt = Instant.now().plus(tusRuntimeConfig.expirationHours(), ChronoUnit.HOURS);
        info.setExpiresAt(expiresAt);

        uploads.put(id, info);

        Path file = safePath(id);
        try {
            if (!Files.exists(file)) {
                Files.createFile(file);
            }
        } catch (IOException e) {
            LOG.errorf(e, "Failed to create upload file for deferred upload %s", id);
            uploads.remove(id);
            return Optional.empty();
        }

        LOG.infof("Created deferred-length upload %s", id);
        return Optional.of(tusBuildTimeConfig.path() + "/" + id);
    }

    @Override
    public boolean setDeferredLength(String id, long length) {
        UploadInfo info = uploads.get(id);
        if (info == null) {
            return false;
        }
        if (!info.isDeferredLength()) {
            return false;
        }
        if (info.getEntityLength() >= 0) {
            return false;
        }
        if (!checkServerSizeConstraint(length)) {
            return false;
        }

        info.setEntityLength(length);
        info.setDeferredLength(false);
        uploadProgressService.startUpload(id, length);
        LOG.infof("Set deferred length for upload %s to %d", id, length);
        return true;
    }

    @Override
    public boolean hasDeferredLength(String id) {
        UploadInfo info = uploads.get(id);
        return info != null && info.isDeferredLength() && info.getEntityLength() < 0;
    }

    @Override
    public Optional<String> mergePartialUploadsWithOwnership(String[] ids,
                                                              Optional<String> uploadMetadata,
                                                              String requiredOwnerId) {
        if (ids == null || ids.length == 0) {
            return Optional.empty();
        }

        LOG.infof("Merging %d partial uploads: %s (requiredOwner=%s)",
                ids.length, String.join(", ", ids), requiredOwnerId);

        // Acquire locks on all partials to prevent concurrent modification
        List<String> lockedIds = new ArrayList<>();
        for (String partialId : ids) {
            if (acquireLock(partialId)) {
                lockedIds.add(partialId);
            } else {
                // Release already-acquired locks
                lockedIds.forEach(this::releaseLock);
                LOG.warnf("Could not acquire lock on partial %s for merge", partialId);
                return Optional.empty();
            }
        }

        try {
            return doMergeWithOwnership(ids, uploadMetadata, requiredOwnerId);
        } finally {
            lockedIds.forEach(this::releaseLock);
        }
    }

    private Optional<String> doMergeWithOwnership(String[] ids,
                                                    Optional<String> uploadMetadata,
                                                    String requiredOwnerId) {
        long totalLength = 0;
        for (String partialId : ids) {
            UploadInfo partialInfo = uploads.get(partialId);
            if (partialInfo == null) {
                LOG.warnf("Partial upload %s not found", partialId);
                return Optional.empty();
            }
            if (!partialInfo.isPartial()) {
                LOG.warnf("Upload %s is not marked as partial", partialId);
                return Optional.empty();
            }
            if (partialInfo.getOffset() != partialInfo.getEntityLength()) {
                LOG.warnf("Partial upload %s is not complete (offset=%d, length=%d)",
                        partialId, partialInfo.getOffset(), partialInfo.getEntityLength());
                return Optional.empty();
            }
            if (requiredOwnerId != null) {
                String ownerId = partialInfo.getUploaderId();
                if (ownerId != null && !ownerId.equals(requiredOwnerId)) {
                    LOG.warnf("Ownership validation failed for partial %s: required=%s, actual=%s",
                            partialId, requiredOwnerId, ownerId);
                    return Optional.empty();
                }
            }
            totalLength += partialInfo.getEntityLength();
        }

        if (!checkServerSizeConstraint(totalLength)) {
            return Optional.empty();
        }

        String finalId = UUID.randomUUID().toString();
        Path finalFile = safePath(finalId);

        try {
            try (var outputStream = Files.newOutputStream(finalFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                for (String partialId : ids) {
                    Path partialFile = safePath(partialId);
                    if (Files.exists(partialFile)) {
                        Files.copy(partialFile, outputStream);
                    } else {
                        Files.deleteIfExists(finalFile);
                        return Optional.empty();
                    }
                }
            }

            UploadInfo finalInfo = new UploadInfo();
            finalInfo.setEntityLength(totalLength);
            finalInfo.setOffset(totalLength);
            finalInfo.setPartial(false);
            uploadMetadata.ifPresent(finalInfo::setMetadata);

            StringBuilder concatValue = new StringBuilder("final;");
            for (int i = 0; i < ids.length; i++) {
                if (i > 0) concatValue.append(" ");
                concatValue.append(tusBuildTimeConfig.path()).append("/").append(ids[i]);
            }
            finalInfo.setUploadConcatMergedValue(concatValue.toString());

            uploads.put(finalId, finalInfo);

            for (String partialId : ids) {
                discardUpload(partialId);
            }

            LOG.infof("Successfully merged %d partials into final upload %s (size=%d)",
                    ids.length, finalId, totalLength);

            return Optional.of(tusBuildTimeConfig.path() + "/" + finalId);

        } catch (IOException e) {
            LOG.errorf(e, "Failed to merge partial uploads into %s", finalId);
            try {
                Files.deleteIfExists(finalFile);
            } catch (IOException cleanupEx) {
                LOG.warnf(cleanupEx, "Failed to clean up partial merge file: %s", finalFile);
            }
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> mergePartialUploadsUnfinished(String[] ids, Optional<String> uploadMetadata) {
        if (ids == null || ids.length == 0) {
            return Optional.empty();
        }

        long totalLength = 0;
        List<String> partialIdList = new ArrayList<>();
        for (String partialId : ids) {
            UploadInfo partialInfo = uploads.get(partialId);
            if (partialInfo == null || !partialInfo.isPartial()) {
                return Optional.empty();
            }
            if (partialInfo.getEntityLength() < 0) {
                return Optional.empty();
            }
            totalLength += partialInfo.getEntityLength();
            partialIdList.add(partialId);
        }

        if (!checkServerSizeConstraint(totalLength)) {
            return Optional.empty();
        }

        String finalId = UUID.randomUUID().toString();

        UploadInfo finalInfo = new UploadInfo();
        finalInfo.setEntityLength(totalLength);
        finalInfo.setOffset(0);
        finalInfo.setPartial(false);
        finalInfo.setFinalConcat(true);
        finalInfo.setPartialIds(partialIdList);
        uploadMetadata.ifPresent(finalInfo::setMetadata);

        Instant expiresAt = Instant.now().plus(tusRuntimeConfig.expirationHours(), ChronoUnit.HOURS);
        finalInfo.setExpiresAt(expiresAt);

        StringBuilder concatValue = new StringBuilder("final;");
        for (int i = 0; i < ids.length; i++) {
            if (i > 0) concatValue.append(" ");
            concatValue.append(tusBuildTimeConfig.path()).append("/").append(ids[i]);
        }
        finalInfo.setUploadConcatMergedValue(concatValue.toString());

        uploads.put(finalId, finalInfo);

        Path finalFile = safePath(finalId);
        try {
            if (!Files.exists(finalFile)) {
                Files.createFile(finalFile);
            }
        } catch (IOException e) {
            LOG.errorf(e, "Failed to create placeholder file for unfinished concat %s", finalId);
            uploads.remove(finalId);
            return Optional.empty();
        }

        return Optional.of(tusBuildTimeConfig.path() + "/" + finalId);
    }

    @Override
    public boolean isConcatReady(String id) {
        UploadInfo info = uploads.get(id);
        if (info == null || !info.isFinalConcat()) {
            return false;
        }
        return info.areAllPartialsComplete(uploads::get);
    }

    @Override
    public boolean finalizeConcatenation(String id) {
        UploadInfo info = uploads.get(id);
        if (info == null || !info.isFinalConcat()) {
            return false;
        }
        if (!info.areAllPartialsComplete(uploads::get)) {
            return false;
        }

        List<String> partialIds = info.getPartialIds();
        if (partialIds == null || partialIds.isEmpty()) {
            return false;
        }

        // Acquire locks on all partials to prevent concurrent modification
        List<String> lockedIds = new ArrayList<>();
        for (String partialId : partialIds) {
            if (acquireLock(partialId)) {
                lockedIds.add(partialId);
            } else {
                lockedIds.forEach(this::releaseLock);
                LOG.warnf("Could not acquire lock on partial %s for finalization", partialId);
                return false;
            }
        }

        try {
            Path finalFile = safePath(id);

            try {
                try (var outputStream = Files.newOutputStream(finalFile,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {
                    for (String partialId : partialIds) {
                        Path partialFile = safePath(partialId);
                        if (Files.exists(partialFile)) {
                            Files.copy(partialFile, outputStream);
                        } else {
                            return false;
                        }
                    }
                }

                info.setOffset(info.getEntityLength());
                info.setFinalConcat(false);

                for (String partialId : partialIds) {
                    discardUpload(partialId);
                }
                info.setPartialIds(null);

                return true;

            } catch (IOException e) {
                LOG.errorf(e, "Failed to finalize concatenation %s", id);
                return false;
            }
        } finally {
            lockedIds.forEach(this::releaseLock);
        }
    }

    @Override
    public boolean checkServerSizeConstraint(Long totalLength) {
        if (totalLength == null) return false;
        return totalLength <= tusRuntimeConfig.maxSize();
    }

    @Override
    public boolean discardUpload(String id) {
        UploadInfo removed = uploads.remove(id);
        uploadProgressService.finishUpload(id);
        activeLocks.remove(id);

        Path file = safePath(id);
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            LOG.warnf(e, "Failed to delete upload file for %s", id);
        }

        return removed != null;
    }

    @Override
    public boolean acquireLock(String id) {
        long now = System.currentTimeMillis();
        Long existing = activeLocks.putIfAbsent(id, now);
        if (existing == null) {
            return true;
        }
        // Check if existing lock has timed out
        if (now - existing > LOCK_TIMEOUT_MS) {
            if (activeLocks.replace(id, existing, now)) {
                LOG.warnf("Reclaimed stale lock for upload %s (held for %d ms)", id, now - existing);
                return true;
            }
        }
        return false;
    }

    @Override
    public void releaseLock(String id) {
        activeLocks.remove(id);
    }

    public void cleanupStaleLocks() {
        long now = System.currentTimeMillis();
        activeLocks.entrySet().removeIf(entry -> {
            boolean stale = now - entry.getValue() > LOCK_TIMEOUT_MS;
            if (stale) {
                LOG.warnf("Removing stale lock for upload %s (held for %d ms)", entry.getKey(), now - entry.getValue());
            }
            return stale;
        });
    }

    @Override
    public boolean validateOffset(String id, long clientOffset) {
        UploadInfo info = uploads.get(id);
        if (info == null) {
            return false;
        }
        return info.getOffset() == clientOffset;
    }

    @Override
    public Uni<Long> writeChunkAsync(String id, long offset, byte[] chunk,
                                      Optional<UploadInfo.ChecksumInfo> checksum) {
        UploadInfo info = uploads.get(id);
        if (info == null) {
            return Uni.createFrom().item(-1L);
        }

        Path file = safePath(id);
        byte[] data = (chunk != null) ? chunk : new byte[0];

        if (checksum.isPresent() && data.length > 0) {
            UploadInfo.ChecksumInfo checksumInfo = checksum.get();
            try {
                if (!validateChecksum(data, checksumInfo)) {
                    return Uni.createFrom().failure(new ChecksumMismatchException("Checksum validation failed"));
                }
            } catch (ChecksumMismatchException e) {
                return Uni.createFrom().failure(e);
            }
        }

        OpenOptions openOptions = new OpenOptions()
                .setWrite(true)
                .setCreate(false);

        return vertx.fileSystem()
                .open(file.toString(), openOptions)
                .flatMap(asyncFile ->
                        asyncFile.write(
                                io.vertx.mutiny.core.buffer.Buffer.buffer(data),
                                offset
                        ).onItem().transform(v -> {
                            long newOffset = offset + data.length;
                            if (newOffset > info.getEntityLength()) {
                                newOffset = info.getEntityLength();
                            }
                            return newOffset;
                        }).eventually(asyncFile::close)
                )
                .onItem().invoke(newOffset -> {
                    info.setOffset(newOffset);
                })
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .onItem().invoke(newOffset -> {
                    if (newOffset == info.getEntityLength()) {
                        uploadProgressService.finishUpload(id);
                        uploadCompletedEvent.fire(new TusUploadCompletedEvent(
                                id, info.getEntityLength(), info.getMetadata(), info.getUploaderId()));
                    }
                })
                .onFailure().invoke(e -> LOG.errorf(e, "Error while writing upload %s to %s", id, file));
    }

    private boolean validateChecksum(byte[] data, UploadInfo.ChecksumInfo checksumInfo) {
        String algorithm = checksumInfo.getAlgorithm().toLowerCase();
        String expectedValue = checksumInfo.getValue();

        try {
            String digestAlgorithm = switch (algorithm) {
                case "sha1" -> "SHA-1";
                case "md5" -> "MD5";
                case "sha256" -> "SHA-256";
                default -> throw new ChecksumMismatchException("Unsupported checksum algorithm: " + algorithm);
            };

            MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);
            byte[] hash = digest.digest(data);
            String computedValue = Base64.getEncoder().encodeToString(hash);

            return computedValue.equals(expectedValue);

        } catch (NoSuchAlgorithmException e) {
            throw new ChecksumMismatchException("Checksum algorithm unavailable: " + algorithm);
        }
    }

    public static class ChecksumMismatchException extends RuntimeException {
        public ChecksumMismatchException(String message) {
            super(message);
        }
    }

    @Override
    public long writeInitialData(String id, byte[] data) {
        if (data == null || data.length == 0) {
            return 0;
        }

        UploadInfo info = uploads.get(id);
        if (info == null) {
            return -1;
        }

        Path file = safePath(id);

        try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            out.write(data);
            long newOffset = Math.min(data.length, info.getEntityLength());
            info.setOffset(newOffset);
            return newOffset;
        } catch (IOException e) {
            LOG.errorf(e, "Failed to write initial data for upload %s", id);
            return -1;
        }
    }

    @Override
    public boolean isExpired(String id) {
        UploadInfo info = uploads.get(id);
        if (info == null) {
            return true;
        }
        Instant expiresAt = info.getExpiresAt();
        if (expiresAt == null) {
            return false;
        }
        return Instant.now().isAfter(expiresAt);
    }

    @Override
    public Optional<Instant> getExpiresAt(String id) {
        UploadInfo info = uploads.get(id);
        if (info == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(info.getExpiresAt());
    }

    @Override
    public List<String> cleanupExpiredUploads() {
        List<String> expiredIds = new ArrayList<>();
        Instant now = Instant.now();

        for (Map.Entry<String, UploadInfo> entry : uploads.entrySet()) {
            UploadInfo info = entry.getValue();
            Instant expiresAt = info.getExpiresAt();
            if (expiresAt != null && now.isAfter(expiresAt)) {
                expiredIds.add(entry.getKey());
            }
        }

        for (String id : expiredIds) {
            LOG.infof("Cleaning up expired upload: %s", id);
            discardUpload(id);
        }

        if (!expiredIds.isEmpty()) {
            LOG.infof("Cleaned up %d expired uploads: %s", expiredIds.size(), String.join(", ", expiredIds));
        }

        return expiredIds;
    }
}
