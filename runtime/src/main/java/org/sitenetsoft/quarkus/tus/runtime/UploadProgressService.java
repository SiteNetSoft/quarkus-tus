package org.sitenetsoft.quarkus.tus.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadProgress;

@ApplicationScoped
public class UploadProgressService {

    private static final Logger LOG = Logger.getLogger(UploadProgressService.class);
    private static final long PROGRESS_TTL_MS = 2 * 60 * 60 * 1000L; // 2 hours

    private final Map<String, ProgressEntry> uploads = new ConcurrentHashMap<>();

    private static class ProgressEntry {
        final UploadProgress progress;
        volatile long lastUpdatedAt;

        ProgressEntry(UploadProgress progress) {
            this.progress = progress;
            this.lastUpdatedAt = System.currentTimeMillis();
        }
    }

    public void startUpload(String uploadId, long totalBytes) {
        uploads.put(uploadId, new ProgressEntry(new UploadProgress(totalBytes)));
    }

    public void updateProgress(String uploadId, long chunkSize) {
        var entry = uploads.get(uploadId);
        if (entry != null) {
            entry.progress.uploadedBytes += chunkSize;
            entry.lastUpdatedAt = System.currentTimeMillis();
        }
    }

    public UploadProgress getProgress(String uploadId) {
        var entry = uploads.get(uploadId);
        return entry != null ? entry.progress : null;
    }

    public void finishUpload(String uploadId) {
        uploads.remove(uploadId);
    }

    public void cleanupExpiredEntries() {
        long now = System.currentTimeMillis();
        List<String> expired = new ArrayList<>();

        for (Map.Entry<String, ProgressEntry> entry : uploads.entrySet()) {
            if (now - entry.getValue().lastUpdatedAt > PROGRESS_TTL_MS) {
                expired.add(entry.getKey());
            }
        }

        for (String id : expired) {
            uploads.remove(id);
        }

        if (!expired.isEmpty()) {
            LOG.infof("Cleaned up %d stale progress entries", expired.size());
        }
    }
}
