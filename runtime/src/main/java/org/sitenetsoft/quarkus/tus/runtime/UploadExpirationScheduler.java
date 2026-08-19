package org.sitenetsoft.quarkus.tus.runtime;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.ratelimit.TusRateLimitService;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.util.List;

@ApplicationScoped
public class UploadExpirationScheduler {

    private static final Logger LOG = Logger.getLogger(UploadExpirationScheduler.class);

    @Inject
    UploadStore uploadStore;

    @Inject
    UploadProgressService uploadProgressService;

    @Inject
    TusRateLimitService rateLimitService;

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    /** Scheduled methods run on a worker thread, so awaiting the store here is fine; bounded anyway. */
    private static final java.time.Duration CLEANUP_TIMEOUT = java.time.Duration.ofMinutes(10);

    @Scheduled(every = "1h", delayed = "5m")
    public void cleanupExpiredUploads() {
        LOG.debug("Running scheduled cleanup of expired uploads");
        List<String> cleaned = uploadStore.cleanupExpiredUploads().await().atMost(CLEANUP_TIMEOUT);
        cleaned.forEach(uploadProgressService::finishUpload);
        if (!cleaned.isEmpty()) {
            LOG.infof("Scheduled cleanup removed %d expired uploads", cleaned.size());
        }
    }

    @Scheduled(every = "1m")
    public void cleanupStaleLocks() {
        uploadStore.cleanupStaleLocks().await().atMost(CLEANUP_TIMEOUT);
    }

    @Scheduled(every = "30m", delayed = "10m")
    public void cleanupStaleProgress() {
        uploadProgressService.cleanupExpiredEntries();
    }

    @Scheduled(every = "30m", delayed = "15m")
    public void cleanupIdleRateLimitBuckets() {
        rateLimitService.cleanupIdleBuckets();
    }

    @Scheduled(every = "1h", delayed = "30m")
    public void cleanupStaleUploads() {
        long staleHours = tusRuntimeConfig.staleUploadHours();
        if (staleHours > 0) {
            List<String> cleaned = uploadStore.cleanupStaleUploads(staleHours).await().atMost(CLEANUP_TIMEOUT);
            cleaned.forEach(uploadProgressService::finishUpload);
            if (!cleaned.isEmpty()) {
                LOG.infof("Scheduled cleanup removed %d stale uploads", cleaned.size());
            }
        }
    }

    @Scheduled(every = "1h", delayed = "45m")
    public void cleanupOrphanFiles() {
        int cleaned = uploadStore.cleanupOrphanFiles().await().atMost(CLEANUP_TIMEOUT);
        if (cleaned > 0) {
            LOG.infof("Scheduled cleanup removed %d orphaned files", cleaned);
        }
    }
}
