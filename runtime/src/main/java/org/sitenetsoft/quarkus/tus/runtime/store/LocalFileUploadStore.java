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
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
    private static final String META_SUFFIX = ".meta";
    private static final String META_TMP_SUFFIX = ".meta.tmp";

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

        reloadPersistedUploads();
    }

    private void persistMetadata(String id, UploadInfo info) {
        try {
            Path tmpFile = uploadBaseDir.resolve(id + META_TMP_SUFFIX);
            Path metaFile = uploadBaseDir.resolve(id + META_SUFFIX);
            Files.writeString(tmpFile, info.toJson(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmpFile, metaFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOG.warnf(e, "Failed to persist metadata for upload %s", id);
        }
    }

    private void deleteMetadata(String id) {
        try {
            Files.deleteIfExists(uploadBaseDir.resolve(id + META_SUFFIX));
            Files.deleteIfExists(uploadBaseDir.resolve(id + META_TMP_SUFFIX));
        } catch (IOException e) {
            LOG.warnf(e, "Failed to delete metadata for upload %s", id);
        }
    }

    private void reloadPersistedUploads() {
        int loaded = 0, expired = 0, skipped = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(uploadBaseDir, "*" + META_SUFFIX)) {
            for (Path metaFile : stream) {
                String fileName = metaFile.getFileName().toString();
                String id = fileName.substring(0, fileName.length() - META_SUFFIX.length());

                if (!org.sitenetsoft.quarkus.tus.runtime.TusUtils.isValidUuid(id)) {
                    skipped++;
                    continue;
                }

                Path dataFile = safePath(id);
                if (!Files.exists(dataFile)) {
                    LOG.warnf("Orphaned metadata file (no data file): %s — removing", fileName);
                    deleteMetadata(id);
                    skipped++;
                    continue;
                }

                try {
                    String json = Files.readString(metaFile);
                    UploadInfo info = UploadInfo.fromJson(json);

                    // Remove expired uploads
                    if (info.getExpiresAt() != null && Instant.now().isAfter(info.getExpiresAt())) {
                        Files.deleteIfExists(dataFile);
                        deleteMetadata(id);
                        expired++;
                        continue;
                    }

                    // Reconcile offset with actual file size
                    long fileSize = Files.size(dataFile);
                    if (info.getOffset() != fileSize && !info.isFinalConcat()) {
                        LOG.warnf("Offset mismatch for upload %s: meta=%d, file=%d — trusting file size",
                                id, info.getOffset(), fileSize);
                        info.setOffset(fileSize);
                    }

                    uploads.put(id, info);
                    loaded++;
                } catch (Exception e) {
                    LOG.warnf(e, "Corrupt metadata file %s — skipping", fileName);
                    skipped++;
                }
            }
        } catch (IOException e) {
            LOG.warnf(e, "Failed to scan for persisted uploads in %s", uploadBaseDir);
        }

        // Clean up orphaned .meta.tmp files
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(uploadBaseDir, "*" + META_TMP_SUFFIX)) {
            for (Path tmpFile : stream) {
                Files.deleteIfExists(tmpFile);
            }
        } catch (IOException e) {
            LOG.warnf(e, "Failed to clean up tmp metadata files");
        }

        if (loaded > 0 || expired > 0 || skipped > 0) {
            LOG.infof("Reloaded persisted uploads: loaded=%d, expired=%d, skipped=%d", loaded, expired, skipped);
        }
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
        info.setLastActivity(Instant.now());
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

        persistMetadata(id, info);
        return Optional.of(tusBuildTimeConfig.path() + "/" + id);
    }

    @Override
    public void setUploaderId(String id, String uploaderId) {
        UploadInfo info = uploads.get(id);
        if (info != null) {
            info.setUploaderId(uploaderId);
            persistMetadata(id, info);
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
        info.setLastActivity(Instant.now());
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

        persistMetadata(id, info);
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
        persistMetadata(id, info);
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
            finalInfo.setUploaderId(requiredOwnerId);
            finalInfo.setLastActivity(Instant.now());
            finalInfo.setExpiresAt(Instant.now().plus(tusRuntimeConfig.expirationHours(), ChronoUnit.HOURS));
            uploadMetadata.ifPresent(finalInfo::setMetadata);

            StringBuilder concatValue = new StringBuilder("final;");
            for (int i = 0; i < ids.length; i++) {
                if (i > 0) concatValue.append(" ");
                concatValue.append(tusBuildTimeConfig.path()).append("/").append(ids[i]);
            }
            finalInfo.setUploadConcatMergedValue(concatValue.toString());

            uploads.put(finalId, finalInfo);
            persistMetadata(finalId, finalInfo);

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
    public Optional<String> mergePartialUploadsUnfinished(String[] ids, Optional<String> uploadMetadata,
                                                          String requiredOwnerId) {
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
            if (requiredOwnerId != null) {
                String ownerId = partialInfo.getUploaderId();
                if (ownerId != null && !ownerId.equals(requiredOwnerId)) {
                    LOG.warnf("Ownership validation failed for partial %s: required=%s, actual=%s",
                            partialId, requiredOwnerId, ownerId);
                    return Optional.empty();
                }
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
        finalInfo.setUploaderId(requiredOwnerId);
        finalInfo.setLastActivity(Instant.now());
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

        persistMetadata(finalId, finalInfo);
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
                persistMetadata(id, info);

                return true;

            } catch (IOException e) {
                LOG.errorf(e, "Failed to finalize concatenation %s — truncating partial merge", id);
                truncateToOffset(finalFile, 0);
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
        deleteMetadata(id);

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
                    info.setLastActivity(Instant.now());
                    persistMetadata(id, info);
                })
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .onItem().invoke(newOffset -> {
                    if (newOffset == info.getEntityLength()) {
                        uploadProgressService.finishUpload(id);
                        uploadCompletedEvent.fire(new TusUploadCompletedEvent(
                                id, info.getEntityLength(), info.getMetadata(), info.getUploaderId()));
                    }
                })
                .onFailure().invoke(e -> {
                    LOG.errorf(e, "Error writing upload %s to %s — truncating to safe offset %d", id, file, offset);
                    truncateToOffset(file, offset);
                });
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

        // Never store more than the declared length: clamping only the recorded offset
        // would leave trailing bytes on disk and report the upload as complete.
        if (info.getEntityLength() >= 0 && data.length > info.getEntityLength()) {
            LOG.warnf("Initial data for upload %s exceeds declared length (%d > %d)",
                    id, data.length, info.getEntityLength());
            return -1;
        }

        Path file = safePath(id);

        try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            out.write(data);
            long newOffset = data.length;
            info.setOffset(newOffset);
            info.setLastActivity(Instant.now());
            persistMetadata(id, info);
            return newOffset;
        } catch (IOException e) {
            LOG.errorf(e, "Failed to write initial data for upload %s — truncating to 0", id);
            truncateToOffset(file, 0);
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

    private void truncateToOffset(Path file, long safeOffset) {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.truncate(safeOffset);
        } catch (IOException truncErr) {
            LOG.warnf(truncErr, "Failed to truncate file %s to offset %d", file, safeOffset);
        }
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

    /**
     * Removes incomplete uploads that have had no activity for the given number of hours.
     */
    public List<String> cleanupStaleUploads(long staleHours) {
        if (staleHours <= 0) {
            return List.of();
        }

        Instant cutoff = Instant.now().minus(staleHours, ChronoUnit.HOURS);
        List<String> staleIds = new ArrayList<>();

        for (Map.Entry<String, UploadInfo> entry : uploads.entrySet()) {
            UploadInfo info = entry.getValue();
            // Only clean up incomplete uploads
            if (info.getOffset() >= info.getEntityLength() && info.getEntityLength() >= 0) {
                continue;
            }
            Instant lastActivity = info.getLastActivity();
            if (lastActivity != null && lastActivity.isBefore(cutoff)) {
                staleIds.add(entry.getKey());
            }
        }

        for (String id : staleIds) {
            LOG.infof("Cleaning up stale upload %s (no activity since %s)", id,
                    uploads.get(id) != null ? uploads.get(id).getLastActivity() : "unknown");
            discardUpload(id);
        }

        if (!staleIds.isEmpty()) {
            LOG.infof("Cleaned up %d stale uploads", staleIds.size());
        }

        return staleIds;
    }

    /**
     * Scans the upload directory for data files with no matching in-memory entry
     * and no .meta sidecar file. These are orphans from crashes or incomplete cleanup.
     */
    public int cleanupOrphanFiles() {
        int cleaned = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(uploadBaseDir)) {
            for (Path file : stream) {
                String name = file.getFileName().toString();
                // Skip metadata files
                if (name.endsWith(META_SUFFIX) || name.endsWith(META_TMP_SUFFIX)) {
                    continue;
                }
                // Only consider UUID-named files
                if (!org.sitenetsoft.quarkus.tus.runtime.TusUtils.isValidUuid(name)) {
                    continue;
                }
                // If there's no in-memory entry and no .meta file, it's an orphan
                if (!uploads.containsKey(name) && !Files.exists(uploadBaseDir.resolve(name + META_SUFFIX))) {
                    LOG.infof("Removing orphan data file: %s", name);
                    Files.deleteIfExists(file);
                    cleaned++;
                }
            }
        } catch (IOException e) {
            LOG.warnf(e, "Failed to scan for orphan files in %s", uploadBaseDir);
        }
        if (cleaned > 0) {
            LOG.infof("Cleaned up %d orphan files", cleaned);
        }
        return cleaned;
    }
}
