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

    static final String URL_NOT_SET_REASON = "quarkus.tus.client.url is not set";

    /**
     * Pure decision logic factored out of the producer method so it can be unit-tested without a CDI
     * container: given whether {@code Instance<TusRequestCustomizer>} resolution is ambiguous, returns
     * the {@link TusClient#unavailable(String)} reason to use, or {@code null} if resolution should
     * proceed normally (not ambiguous).
     */
    static String ambiguousCustomizerReason(boolean isAmbiguous) {
        if (!isAmbiguous) {
            return null;
        }
        return "Multiple TusRequestCustomizer beans are present; the client cannot pick one. "
                + "Leave exactly one TusRequestCustomizer bean, or build TusClientOptions "
                + "programmatically with an explicit customizer.";
    }

    @Produces
    @ApplicationScoped
    public TusClient tusClient(Vertx vertx, TusClientRuntimeConfig config,
            Instance<TusRequestCustomizer> customizers) {
        if (config.url().isEmpty()) {
            return TusClient.unavailable(URL_NOT_SET_REASON);
        }
        String ambiguityReason = ambiguousCustomizerReason(customizers.isAmbiguous());
        if (ambiguityReason != null) {
            return TusClient.unavailable(ambiguityReason);
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
