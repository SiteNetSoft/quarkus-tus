package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.client.runtime.TusClient;
import org.sitenetsoft.quarkus.tus.client.runtime.TusUploadRequest;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadResult;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class TusClientInjectionTest {

    @Inject
    TusClient tusClient;

    @Test
    void injectedClientUploadsToTheInRepoServer() {
        TusUploadResult result = tusClient.upload(TusUploadRequest.builder(
                        UploadSource.oneShot(Multi.createFrom().item(Buffer.buffer("injected!")), 9)).build())
                .await().atMost(Duration.ofSeconds(10));

        assertEquals(9, result.bytesUploaded());
        // offset via the low level proves the server really has it
        assertEquals(9L, tusClient.protocol().offset(result.url()).await().atMost(Duration.ofSeconds(5)));
    }
}
