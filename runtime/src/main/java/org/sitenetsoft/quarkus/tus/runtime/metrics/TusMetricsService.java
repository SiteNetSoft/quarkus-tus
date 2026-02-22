package org.sitenetsoft.quarkus.tus.runtime.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.sitenetsoft.quarkus.tus.runtime.event.*;

@ApplicationScoped
public class TusMetricsService {

    @Inject
    Instance<MeterRegistry> registryInstance;

    public void onCreated(@Observes TusUploadCreatedEvent event) {
        ifPresent(registry -> Counter.builder("tus.uploads.created")
                .description("Total uploads created")
                .register(registry)
                .increment());
    }

    public void onCompleted(@Observes TusUploadCompletedEvent event) {
        ifPresent(registry -> {
            Counter.builder("tus.uploads.completed")
                    .description("Total uploads completed")
                    .register(registry)
                    .increment();
            DistributionSummary.builder("tus.uploads.bytes")
                    .description("Upload sizes in bytes")
                    .register(registry)
                    .record(event.totalSize());
        });
    }

    public void onTerminated(@Observes TusUploadTerminatedEvent event) {
        ifPresent(registry -> Counter.builder("tus.uploads.terminated")
                .description("Total uploads terminated")
                .register(registry)
                .increment());
    }

    public void onChunk(@Observes TusChunkReceivedEvent event) {
        ifPresent(registry -> Counter.builder("tus.chunks.received")
                .description("Total chunks received")
                .register(registry)
                .increment());
    }

    public void onConcat(@Observes TusConcatenationCompletedEvent event) {
        ifPresent(registry -> Counter.builder("tus.uploads.concatenated")
                .description("Total concatenation merges")
                .register(registry)
                .increment());
    }

    private void ifPresent(java.util.function.Consumer<MeterRegistry> action) {
        if (registryInstance.isResolvable()) {
            action.accept(registryInstance.get());
        }
    }
}
