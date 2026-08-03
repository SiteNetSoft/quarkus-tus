package org.sitenetsoft.quarkus.tus.it;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import org.sitenetsoft.quarkus.tus.runtime.UploadProgressService;
import org.sitenetsoft.quarkus.tus.runtime.config.TusBuildTimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.event.TusUploadCompletedEvent;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.store.LocalFileUploadStore;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
@Alternative
public class InMemoryUploadStore implements UploadStore {

    private final Map<String, UploadInfo> uploads = new ConcurrentHashMap<>();
    private final Map<String, byte[]> dataStore = new ConcurrentHashMap<>();
    private final Set<String> activeLocks = ConcurrentHashMap.newKeySet();

    @Inject
    UploadProgressService uploadProgressService;

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    @Inject
    TusBuildTimeConfig tusBuildTimeConfig;

    @Inject
    Event<TusUploadCompletedEvent> uploadCompletedEvent;

    public boolean hasData(String id) {
        return dataStore.containsKey(id);
    }

    public byte[] getData(String id) {
        return dataStore.get(id);
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
        dataStore.put(id, new byte[0]);
        uploadProgressService.startUpload(id, totalLength);

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
        dataStore.put(id, new byte[0]);

        return Optional.of(tusBuildTimeConfig.path() + "/" + id);
    }

    @Override
    public boolean setDeferredLength(String id, long length) {
        UploadInfo info = uploads.get(id);
        if (info == null || !info.isDeferredLength() || info.getEntityLength() >= 0) {
            return false;
        }
        if (!checkServerSizeConstraint(length)) {
            return false;
        }

        info.setEntityLength(length);
        info.setDeferredLength(false);
        uploadProgressService.startUpload(id, length);
        return true;
    }

    @Override
    public boolean hasDeferredLength(String id) {
        UploadInfo info = uploads.get(id);
        return info != null && info.isDeferredLength() && info.getEntityLength() < 0;
    }

    @Override
    public Optional<String> mergePartialUploadsWithOwnership(String[] ids, Optional<String> uploadMetadata, String requiredOwnerId) {
        if (ids == null || ids.length == 0) {
            return Optional.empty();
        }

        long totalLength = 0;
        for (String partialId : ids) {
            UploadInfo partialInfo = uploads.get(partialId);
            if (partialInfo == null || !partialInfo.isPartial()) {
                return Optional.empty();
            }
            if (partialInfo.getOffset() != partialInfo.getEntityLength()) {
                return Optional.empty();
            }
            if (requiredOwnerId != null) {
                String ownerId = partialInfo.getUploaderId();
                if (ownerId != null && !ownerId.equals(requiredOwnerId)) {
                    return Optional.empty();
                }
            }
            totalLength += partialInfo.getEntityLength();
        }

        if (!checkServerSizeConstraint(totalLength)) {
            return Optional.empty();
        }

        String finalId = UUID.randomUUID().toString();

        // Merge data
        byte[] mergedData = new byte[(int) totalLength];
        int pos = 0;
        for (String partialId : ids) {
            byte[] partialData = dataStore.get(partialId);
            if (partialData == null) {
                return Optional.empty();
            }
            System.arraycopy(partialData, 0, mergedData, pos, partialData.length);
            pos += partialData.length;
        }
        dataStore.put(finalId, mergedData);

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

        return Optional.of(tusBuildTimeConfig.path() + "/" + finalId);
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
        dataStore.put(finalId, new byte[0]);

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

        // Merge data in memory
        int totalLen = 0;
        for (String partialId : partialIds) {
            byte[] d = dataStore.get(partialId);
            if (d == null) return false;
            totalLen += d.length;
        }
        byte[] mergedData = new byte[totalLen];
        int pos = 0;
        for (String partialId : partialIds) {
            byte[] d = dataStore.get(partialId);
            System.arraycopy(d, 0, mergedData, pos, d.length);
            pos += d.length;
        }
        dataStore.put(id, mergedData);

        info.setOffset(info.getEntityLength());
        info.setFinalConcat(false);

        for (String partialId : partialIds) {
            discardUpload(partialId);
        }
        info.setPartialIds(null);

        return true;
    }

    @Override
    public boolean checkServerSizeConstraint(Long totalLength) {
        if (totalLength == null) return false;
        return totalLength <= tusRuntimeConfig.maxSize();
    }

    @Override
    public boolean discardUpload(String id) {
        UploadInfo removed = uploads.remove(id);
        dataStore.remove(id);
        uploadProgressService.finishUpload(id);
        activeLocks.remove(id);
        return removed != null;
    }

    @Override
    public boolean acquireLock(String id) {
        return activeLocks.add(id);
    }

    @Override
    public void releaseLock(String id) {
        activeLocks.remove(id);
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
    public Uni<Long> writeChunkAsync(String id, long offset, byte[] chunk, Optional<UploadInfo.ChecksumInfo> checksum) {
        UploadInfo info = uploads.get(id);
        if (info == null) {
            return Uni.createFrom().item(-1L);
        }

        byte[] data = (chunk != null) ? chunk : new byte[0];

        if (checksum.isPresent() && data.length > 0) {
            UploadInfo.ChecksumInfo checksumInfo = checksum.get();
            if (!validateChecksum(data, checksumInfo)) {
                return Uni.createFrom().failure(
                        new LocalFileUploadStore.ChecksumMismatchException("Checksum validation failed"));
            }
        }

        // Append data in memory
        byte[] existing = dataStore.getOrDefault(id, new byte[0]);
        byte[] combined = new byte[existing.length + data.length];
        System.arraycopy(existing, 0, combined, 0, existing.length);
        System.arraycopy(data, 0, combined, existing.length, data.length);
        dataStore.put(id, combined);

        long newOffset = offset + data.length;
        if (newOffset > info.getEntityLength()) {
            newOffset = info.getEntityLength();
        }
        info.setOffset(newOffset);

        if (newOffset == info.getEntityLength()) {
            uploadProgressService.finishUpload(id);
            uploadCompletedEvent.fire(new TusUploadCompletedEvent(
                    id, info.getEntityLength(), info.getMetadata(), info.getUploaderId()));
        }

        return Uni.createFrom().item(newOffset);
    }

    private boolean validateChecksum(byte[] data, UploadInfo.ChecksumInfo checksumInfo) {
        String algorithm = checksumInfo.getAlgorithm().toLowerCase();
        String expectedValue = checksumInfo.getValue();

        try {
            String digestAlgorithm = switch (algorithm) {
                case "sha1" -> "SHA-1";
                case "md5" -> "MD5";
                case "sha256" -> "SHA-256";
                default -> throw new LocalFileUploadStore.ChecksumMismatchException(
                        "Unsupported checksum algorithm: " + algorithm);
            };

            MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);
            byte[] hash = digest.digest(data);
            String computedValue = Base64.getEncoder().encodeToString(hash);

            return computedValue.equals(expectedValue);

        } catch (NoSuchAlgorithmException e) {
            throw new LocalFileUploadStore.ChecksumMismatchException("Checksum algorithm unavailable: " + algorithm);
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

        byte[] existing = dataStore.getOrDefault(id, new byte[0]);
        byte[] combined = new byte[existing.length + data.length];
        System.arraycopy(existing, 0, combined, 0, existing.length);
        System.arraycopy(data, 0, combined, existing.length, data.length);
        dataStore.put(id, combined);

        long newOffset = Math.min(data.length, info.getEntityLength());
        info.setOffset(newOffset);
        return newOffset;
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
            discardUpload(id);
        }

        return expiredIds;
    }
}
