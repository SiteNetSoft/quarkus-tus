package org.sitenetsoft.quarkus.tus.deployment;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import org.sitenetsoft.quarkus.tus.runtime.TusUploadResource;
import org.sitenetsoft.quarkus.tus.runtime.UploadExpirationScheduler;
import org.sitenetsoft.quarkus.tus.runtime.UploadProgressService;
import org.sitenetsoft.quarkus.tus.runtime.auth.TusAuthFilter;
import org.sitenetsoft.quarkus.tus.runtime.config.TusBuildTimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.ratelimit.TusRateLimitFilter;
import org.sitenetsoft.quarkus.tus.runtime.ratelimit.TusRateLimitService;
import org.sitenetsoft.quarkus.tus.runtime.devui.TusDevUIJsonRpcService;
import org.sitenetsoft.quarkus.tus.runtime.health.TusHealthCheck;
import org.sitenetsoft.quarkus.tus.runtime.metrics.TusMetricsService;
import org.sitenetsoft.quarkus.tus.runtime.sse.TusProgressResource;
import org.sitenetsoft.quarkus.tus.runtime.sse.TusSseResource;
import org.sitenetsoft.quarkus.tus.runtime.sse.TusSseService;
import org.sitenetsoft.quarkus.tus.runtime.event.*;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadProgress;
import org.sitenetsoft.quarkus.tus.runtime.store.LocalFileUploadStore;

class TusProcessor {

    private static final String FEATURE = "tus";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    ReflectiveClassBuildItem reflectiveClasses() {
        return ReflectiveClassBuildItem.builder(
                UploadInfo.class,
                UploadInfo.ChecksumInfo.class,
                UploadProgress.class,
                TusUploadCreatedEvent.class,
                TusChunkReceivedEvent.class,
                TusUploadCompletedEvent.class,
                TusUploadTerminatedEvent.class,
                TusConcatenationCompletedEvent.class
        ).methods().fields().build();
    }

    @BuildStep
    AdditionalBeanBuildItem tusCoreBeans() {
        return AdditionalBeanBuildItem.builder()
                .setUnremovable()
                .addBeanClasses(
                        TusUploadResource.class,
                        LocalFileUploadStore.class,
                        UploadProgressService.class,
                        UploadExpirationScheduler.class,
                        TusMetricsService.class,
                        TusDevUIJsonRpcService.class,
                        TusRateLimitService.class
                )
                .build();
    }

    @BuildStep
    AdditionalBeanBuildItem tusSseBeans(TusBuildTimeConfig config) {
        if (!config.sseEnabled()) {
            return new AdditionalBeanBuildItem.Builder().build();
        }
        return AdditionalBeanBuildItem.builder()
                .setUnremovable()
                .addBeanClasses(
                        TusSseService.class,
                        TusSseResource.class,
                        TusProgressResource.class
                )
                .build();
    }

    @BuildStep
    AdditionalBeanBuildItem tusHealthCheck(Capabilities capabilities) {
        if (!capabilities.isPresent(Capability.SMALLRYE_HEALTH)) {
            return new AdditionalBeanBuildItem.Builder().build();
        }
        return AdditionalBeanBuildItem.builder()
                .setUnremovable()
                .addBeanClasses(TusHealthCheck.class)
                .build();
    }

    @BuildStep
    AdditionalBeanBuildItem tusRateLimitFilter(TusBuildTimeConfig config) {
        if (!config.rateLimitEnabled()) {
            return new AdditionalBeanBuildItem.Builder().build();
        }
        return AdditionalBeanBuildItem.builder()
                .setUnremovable()
                .addBeanClasses(TusRateLimitFilter.class)
                .build();
    }

    @BuildStep
    AdditionalBeanBuildItem tusAuthFilter(TusBuildTimeConfig config) {
        if (!config.authEnabled()) {
            return new AdditionalBeanBuildItem.Builder().build();
        }
        return AdditionalBeanBuildItem.builder()
                .setUnremovable()
                .addBeanClasses(TusAuthFilter.class)
                .build();
    }
}
