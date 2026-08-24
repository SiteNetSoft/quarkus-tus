package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.sitenetsoft.quarkus.tus.client.runtime.TusClient;
import org.sitenetsoft.quarkus.tus.client.runtime.TusClientOptions;

/**
 * Drives a programmatic {@link TusClient} against the packaged (JVM or native) server. CDI
 * injection is not available in {@code @QuarkusIntegrationTest} mode, so this builds its own
 * {@link Vertx} and {@link TusClient} instead of the {@code @Inject} that
 * {@link TusClientInjectionTest} uses under {@code @QuarkusTest}. The native profile's server has
 * the same 1024-byte {@code max-chunk-size} as the JVM one; the 9-byte upload in the shared test
 * stays under it either way.
 */
@QuarkusIntegrationTest
class TusClientIT extends TusClientTestBase {

    private Vertx vertx;
    private TusClient tusClient;

    @BeforeEach
    void createClient() {
        vertx = Vertx.vertx();
        tusClient = TusClient.create(vertx, TusClientOptions.builder(baseUrl()).build());
    }

    @AfterEach
    void closeClient() {
        if (tusClient != null) {
            tusClient.close();
        }
        if (vertx != null) {
            // Awaited so a future second test in this class can't start against a Vertx that's
            // still mid-close from the previous one.
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    private String baseUrl() {
        return "http://localhost:" + RestAssured.port + "/tus";
    }

    @Override
    protected TusClient client() {
        return tusClient;
    }
}
