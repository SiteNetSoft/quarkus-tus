package org.sitenetsoft.quarkus.tus.deployment;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import org.sitenetsoft.quarkus.tus.runtime.TusUploadResource;
import org.sitenetsoft.quarkus.tus.runtime.UploadExpirationScheduler;
import org.sitenetsoft.quarkus.tus.runtime.UploadProgressService;
import org.sitenetsoft.quarkus.tus.runtime.auth.TusAuthFilter;
import org.sitenetsoft.quarkus.tus.runtime.config.TusBuildTimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.sse.TusProgressResource;
import org.sitenetsoft.quarkus.tus.runtime.sse.TusSseResource;
import org.sitenetsoft.quarkus.tus.runtime.sse.TusSseService;
import org.sitenetsoft.quarkus.tus.runtime.store.LocalFileUploadStore;

class TusProcessor {

    private static final String FEATURE = "tus";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    AdditionalBeanBuildItem tusCoreBeans() {
        return AdditionalBeanBuildItem.builder()
                .setUnremovable()
                .addBeanClasses(
                        TusUploadResource.class,
                        LocalFileUploadStore.class,
                        UploadProgressService.class,
                        UploadExpirationScheduler.class
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
