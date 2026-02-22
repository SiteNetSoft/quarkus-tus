package org.sitenetsoft.quarkus.tus.runtime.health;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

@Readiness
@ApplicationScoped
public class TusHealthCheck implements HealthCheck {

    @Inject
    UploadStore uploadStore;

    @Override
    public HealthCheckResponse call() {
        try {
            uploadStore.findUploadInfo("health-check-probe");
            return HealthCheckResponse.named("TUS Upload Store").up().build();
        } catch (Exception e) {
            return HealthCheckResponse.named("TUS Upload Store").down().withData("error", e.getMessage()).build();
        }
    }
}
