package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.store.LocalFileUploadStore;
import org.sitenetsoft.quarkus.tus.tck.AbstractUploadStoreContractTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** The bundled store must pass its own contract. */
@QuarkusTest
class LocalFileUploadStoreContractTest extends AbstractUploadStoreContractTest {

    @Inject
    UploadStore uploadStore;

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    @Override
    protected UploadStore store() {
        return uploadStore;
    }

    @Override
    protected Optional<byte[]> readBytes(String id) {
        Path file = Path.of(tusRuntimeConfig.store().local().uploadDir(), id);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Files.readAllBytes(file));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void storeUnderTestIsTheLocalFileStore() {
        assertInstanceOf(LocalFileUploadStore.class, uploadStore);
    }
}
