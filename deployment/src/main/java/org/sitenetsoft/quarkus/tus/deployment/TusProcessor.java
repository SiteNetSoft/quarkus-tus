package org.sitenetsoft.quarkus.tus.deployment;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.AdditionalIndexedClassesBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import io.quarkus.runtime.configuration.ConfigurationException;
import org.sitenetsoft.quarkus.tus.runtime.TusMethodOverrideFilter;
import org.sitenetsoft.quarkus.tus.runtime.TusUploadAuthorizer;
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

import java.util.ArrayList;
import java.util.List;

class TusProcessor {

    private static final String FEATURE = "tus";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    /**
     * JAX-RS needs a constant {@code @Path}, so the endpoints cannot be relocated by
     * configuration. Failing the build beats the previous behaviour, where a changed path
     * left the endpoints in place but stopped the auth and rate-limit filters from matching
     * them — silently serving TUS unauthenticated.
     */
    private static void validateTusPath(TusBuildTimeConfig config) {
        if (!TusUploadResource.TUS_PATH.equals(config.path())) {
            throw new ConfigurationException(
                    "quarkus.tus.path is set to '" + config.path() + "' but the TUS endpoints are "
                            + "mounted at '" + TusUploadResource.TUS_PATH + "' and cannot be moved. "
                            + "Remove the property, or place the application behind a reverse proxy "
                            + "that rewrites the prefix.");
        }
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

    /**
     * The runtime module deliberately ships no {@code META-INF/beans.xml}, so its classes are
     * not part of the application index. JAX-RS resources and providers must therefore be
     * indexed explicitly, and only when the corresponding feature is enabled — that is what
     * keeps the conditional registration below meaningful.
     */
    @BuildStep
    void indexedJaxRsClasses(TusBuildTimeConfig config,
                             Capabilities capabilities,
                             BuildProducer<AdditionalIndexedClassesBuildItem> producer) {
        validateTusPath(config);

        List<String> classNames = new ArrayList<>();
        classNames.add(TusUploadResource.class.getName());

        if (config.sseEnabled()) {
            classNames.add(TusSseResource.class.getName());
            classNames.add(TusProgressResource.class.getName());
        }
        if (config.authEnabled()) {
            classNames.add(TusAuthFilter.class.getName());
        }
        if (config.rateLimitEnabled()) {
            classNames.add(TusRateLimitFilter.class.getName());
        }
        if (config.methodOverrideEnabled()) {
            classNames.add(TusMethodOverrideFilter.class.getName());
        }
        if (capabilities.isPresent(Capability.SMALLRYE_HEALTH)) {
            classNames.add(TusHealthCheck.class.getName());
        }

        producer.produce(new AdditionalIndexedClassesBuildItem(classNames.toArray(new String[0])));
    }

    @BuildStep
    AdditionalBeanBuildItem tusCoreBeans() {
        return AdditionalBeanBuildItem.builder()
                .setUnremovable()
                .addBeanClasses(
                        TusUploadResource.class,
                        TusUploadAuthorizer.class,
                        LocalFileUploadStore.class,
                        UploadProgressService.class,
                        UploadExpirationScheduler.class,
                        TusDevUIJsonRpcService.class,
                        TusRateLimitService.class
                )
                .build();
    }

    /**
     * TusMetricsService references Micrometer types directly and Micrometer is only a
     * compileOnly dependency, so it must not be registered in applications that do not
     * have the Micrometer extension — its event observers would fail to load.
     */
    @BuildStep
    AdditionalBeanBuildItem tusMetrics(Capabilities capabilities) {
        if (!capabilities.isPresent(Capability.METRICS)) {
            return new AdditionalBeanBuildItem.Builder().build();
        }
        return AdditionalBeanBuildItem.builder()
                .setUnremovable()
                .addBeanClasses(TusMetricsService.class)
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
    AdditionalBeanBuildItem tusMethodOverrideFilter(TusBuildTimeConfig config) {
        if (!config.methodOverrideEnabled()) {
            return new AdditionalBeanBuildItem.Builder().build();
        }
        return AdditionalBeanBuildItem.builder()
                .setUnremovable()
                .addBeanClasses(TusMethodOverrideFilter.class)
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
