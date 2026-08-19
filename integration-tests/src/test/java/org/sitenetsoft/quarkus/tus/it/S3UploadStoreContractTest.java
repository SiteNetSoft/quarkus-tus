package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.tck.AbstractUploadStoreContractTest;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** The S3 sample store through the SPI contract, against the S3 at $TUS_S3_ENDPOINT (e.g. MinIO). */
@QuarkusTest
@TestProfile(S3UploadStoreContractTest.S3Profile.class)
@EnabledIfEnvironmentVariable(named = "TUS_S3_ENDPOINT", matches = ".+")
class S3UploadStoreContractTest extends AbstractUploadStoreContractTest {

    public static class S3Profile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(S3UploadStore.class);
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.tus.max-chunk-size", String.valueOf(512L * 1024 * 1024),
                    "quarkus.tus.max-size", String.valueOf(1024L * 1024 * 1024),
                    "quarkus.http.limits.max-body-size", "1G");
        }
    }

    @Inject
    UploadStore uploadStore;

    @Override
    protected UploadStore store() {
        return uploadStore;
    }

    @Override
    protected Optional<byte[]> readBytes(String id) {
        return ((S3UploadStore) uploadStore).readBytes(id);
    }
}
