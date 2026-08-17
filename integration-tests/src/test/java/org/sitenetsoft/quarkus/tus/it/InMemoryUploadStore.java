package org.sitenetsoft.quarkus.tus.it;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.BufferingUploadStore;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadNotFoundException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The simplest possible {@link UploadStore}: records and bytes in maps. It exists to prove that
 * a store needs to know nothing about the protocol — no events, no progress bookkeeping, no
 * checksums, no configuration — and to exercise {@link BufferingUploadStore}.
 */
@ApplicationScoped
@Alternative
public class InMemoryUploadStore extends BufferingUploadStore {

    private final Map<String, UploadInfo> uploads = new ConcurrentHashMap<>();
    private final Map<String, byte[]> dataStore = new ConcurrentHashMap<>();
    private final Set<String> activeLocks = ConcurrentHashMap.newKeySet();

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
    public String createUpload(UploadInfo info) {
        String id = UUID.randomUUID().toString();
        uploads.put(id, info);
        dataStore.put(id, new byte[0]);
        return id;
    }

    @Override
    public void updateUploadInfo(String id, UploadInfo info) {
        if (uploads.containsKey(id)) {
            uploads.put(id, info);
        }
    }

    @Override
    protected void appendBytes(String id, long offset, byte[] data) {
        byte[] existing = dataStore.getOrDefault(id, new byte[0]);
        byte[] combined = new byte[existing.length + data.length];
        System.arraycopy(existing, 0, combined, 0, existing.length);
        System.arraycopy(data, 0, combined, existing.length, data.length);
        dataStore.put(id, combined);
    }

    @Override
    public Uni<Void> concatenate(String finalId, List<String> sourceIds) {
        UploadInfo finalInfo = uploads.get(finalId);
        if (finalInfo == null) {
            return Uni.createFrom().failure(new UploadNotFoundException(finalId));
        }
        ByteArrayOutputStream merged = new ByteArrayOutputStream();
        for (String sourceId : sourceIds) {
            byte[] bytes = dataStore.get(sourceId);
            if (bytes == null) {
                return Uni.createFrom().failure(new UploadNotFoundException(sourceId));
            }
            merged.writeBytes(bytes);
        }
        dataStore.put(finalId, merged.toByteArray());
        finalInfo.setOffset(finalInfo.getEntityLength());
        finalInfo.setFinalConcat(false);
        finalInfo.setPartialIds(null);
        finalInfo.setLastActivity(Instant.now());
        return Uni.createFrom().voidItem();
    }

    @Override
    public boolean discardUpload(String id) {
        if (!activeLocks.add(id)) {
            return false;
        }
        try {
            UploadInfo removed = uploads.remove(id);
            dataStore.remove(id);
            return removed != null;
        } finally {
            activeLocks.remove(id);
        }
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
    public List<String> cleanupExpiredUploads() {
        Instant now = Instant.now();
        List<String> cleaned = new ArrayList<>();
        for (Map.Entry<String, UploadInfo> entry : uploads.entrySet()) {
            Instant expiresAt = entry.getValue().getExpiresAt();
            if (expiresAt != null && now.isAfter(expiresAt) && discardUpload(entry.getKey())) {
                cleaned.add(entry.getKey());
            }
        }
        return cleaned;
    }
}
