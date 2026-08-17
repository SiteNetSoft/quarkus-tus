package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.tck.AbstractUploadStoreContractTest;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * The in-memory store must pass the contract too — it is the proof that a store built on
 * {@link org.sitenetsoft.quarkus.tus.runtime.spi.BufferingUploadStore} with no protocol
 * knowledge at all is a correct store.
 */
@QuarkusTest
@TestProfile(TusCustomStoreTest.CustomStoreProfile.class)
class InMemoryUploadStoreContractTest extends AbstractUploadStoreContractTest {

    @Inject
    UploadStore uploadStore;

    @Override
    protected UploadStore store() {
        return uploadStore;
    }

    @Override
    protected Optional<byte[]> readBytes(String id) {
        return Optional.ofNullable(((InMemoryUploadStore) uploadStore).getData(id));
    }

    @Test
    void storeUnderTestIsTheInMemoryStore() {
        assertInstanceOf(InMemoryUploadStore.class, uploadStore);
    }
}
