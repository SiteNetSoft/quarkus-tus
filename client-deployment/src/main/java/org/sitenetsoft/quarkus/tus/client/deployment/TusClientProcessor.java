package org.sitenetsoft.quarkus.tus.client.deployment;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import org.sitenetsoft.quarkus.tus.client.runtime.TusClientProducer;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusServerCapabilities;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUpload;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadProgress;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadResult;

class TusClientProcessor {

    private static final String FEATURE = "tus-client";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    AdditionalBeanBuildItem additionalBeans() {
        return AdditionalBeanBuildItem.unremovableOf(TusClientProducer.class);
    }

    @BuildStep
    ReflectiveClassBuildItem reflectiveModelClasses() {
        return ReflectiveClassBuildItem.builder(
                        TusUpload.class, TusServerCapabilities.class, TusUploadResult.class, TusUploadProgress.class)
                .methods().fields().build();
    }
}
