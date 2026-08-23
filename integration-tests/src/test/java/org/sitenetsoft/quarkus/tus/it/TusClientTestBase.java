package org.sitenetsoft.quarkus.tus.it;

import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.client.runtime.TusClient;
import org.sitenetsoft.quarkus.tus.client.runtime.TusUploadRequest;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadResult;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Upload-and-verify, driven by whatever {@link TusClient} the subclass provides: a CDI-injected
 * one under {@code @QuarkusTest}, or a programmatic one built against the packaged server under
 * {@code @QuarkusIntegrationTest} (CDI injection is not available in IT mode).
 */
abstract class TusClientTestBase {

    protected abstract TusClient client();

    @Test
    void clientUploadsToTheServer() {
        TusUploadResult result = client().upload(TusUploadRequest.builder(
                        UploadSource.oneShot(Multi.createFrom().item(Buffer.buffer("injected!")), 9)).build())
                .await().atMost(Duration.ofSeconds(10));

        assertEquals(9, result.bytesUploaded());
        // offset via the low level proves the server really has it
        assertEquals(9L, client().protocol().offset(result.url()).await().atMost(Duration.ofSeconds(5)));
    }
}
