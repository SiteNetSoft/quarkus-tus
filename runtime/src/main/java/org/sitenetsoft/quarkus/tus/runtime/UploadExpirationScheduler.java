package org.sitenetsoft.quarkus.tus.runtime;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.ratelimit.TusRateLimitService;
import org.sitenetsoft.quarkus.tus.runtime.store.LocalFileUploadStore;
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

    @Scheduled(every = "1h", delayed = "5m")
    public void cleanupExpiredUploads() {
        LOG.debug("Running scheduled cleanup of expired uploads");
        List<String> cleaned = uploadStore.cleanupExpiredUploads();
        if (!cleaned.isEmpty()) {
            LOG.infof("Scheduled cleanup removed %d expired uploads", cleaned.size());
        }
    }

    @Scheduled(every = "1m")
    public void cleanupStaleLocks() {
        if (uploadStore instanceof LocalFileUploadStore localStore) {
            localStore.cleanupStaleLocks();
        }
    }

    @Scheduled(every = "30m", delayed = "10m")
    public void cleanupStaleProgress() {
        uploadProgressService.cleanupExpiredEntries();
    }

    @Scheduled(every = "30m", delayed = "15m")
    public void cleanupIdleRateLimitBuckets() {
        rateLimitService.cleanupIdleBuckets();
    }
}
