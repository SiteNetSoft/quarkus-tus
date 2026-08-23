package org.sitenetsoft.quarkus.tus.client.runtime;

import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import org.sitenetsoft.quarkus.tus.client.runtime.config.TusClientRuntimeConfig;

/**
 * Produces the CDI-scoped {@link TusClient} from {@link TusClientRuntimeConfig}.
 *
 * <p>Neither an unset {@code quarkus.tus.client.url} nor an ambiguous {@link TusRequestCustomizer}
 * fails the boot: both conditions instead produce a {@link TusClient#unavailable(String)} shim that
 * fails on first use, matching the config-validation style used by the server side of this project
 * (fail loud, but only when the feature is actually exercised).
 */
@ApplicationScoped
public class TusClientProducer {

    @Produces
    @ApplicationScoped
    public TusClient tusClient(Vertx vertx, TusClientRuntimeConfig config,
            Instance<TusRequestCustomizer> customizers) {
        if (config.url().isEmpty()) {
            return TusClient.unavailable("quarkus.tus.client.url is not set");
        }
        if (customizers.isAmbiguous()) {
            return TusClient.unavailable(
                    "Multiple TusRequestCustomizer beans are present; the client cannot pick one. "
                            + "Leave exactly one TusRequestCustomizer bean, or build TusClientOptions "
                            + "programmatically with an explicit customizer.");
        }

        TusClientOptions.Builder builder = TusClientOptions.builder(config.url().get())
                .chunkSize(config.chunkSize())
                .maxRetries(config.maxRetries())
                .retryBackoff(config.retryBackoff())
                .retryBackoffMax(config.retryBackoffMax())
                .parallelism(config.parallelism());
        config.checksumAlgorithm().ifPresent(builder::checksumAlgorithm);
        config.connectTimeout().ifPresent(builder::connectTimeout);
        config.requestTimeout().ifPresent(builder::requestTimeout);
        if (!customizers.isUnsatisfied()) {
            builder.customizer(customizers.get());
        }

        return TusClient.create(vertx, builder.build());
    }

    public void disposeTusClient(@Disposes TusClient tusClient) {
        tusClient.close();
    }
}
