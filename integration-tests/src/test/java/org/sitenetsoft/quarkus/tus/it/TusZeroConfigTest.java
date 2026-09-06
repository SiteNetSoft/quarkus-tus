package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Zero means "off" for every duration in the configuration, the way {@code stale-upload-hours}
 * already documents it. It used to mean the opposite for these two: an upload with
 * {@code expiration-hours=0} expired the instant it was created (every HEAD answered 410), and
 * {@code lock-timeout-seconds=0} made every lock reclaimable by the next request, which is no
 * lock at all.
 */
@QuarkusTest
@TestProfile(TusZeroConfigTest.ZeroDurationsProfile.class)
class TusZeroConfigTest {

    public static class ZeroDurationsProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.tus.expiration-hours", "0",
                    "quarkus.tus.lock-timeout-seconds", "0");
        }
    }

    @Inject
    UploadStore uploadStore;

    @Test
    void zeroExpirationHoursMeansUploadsNeverExpire() {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Upload-Expires", nullValue())
                .extract().header("Location");

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Expires", nullValue());

        String id = location.substring(location.lastIndexOf('/') + 1);
        assertNull(Stores.find(uploadStore, id).orElseThrow().getExpiresAt(), "no deadline should be recorded");
        assertTrue(Stores.await(uploadStore.cleanupExpiredUploads()).isEmpty(),
                "the expiry sweep must leave never-expiring uploads alone");
        assertTrue(Stores.find(uploadStore, id).isPresent());
    }

    @Test
    void zeroLockTimeoutMeansLocksAreNeverReclaimed() throws Exception {
        UploadInfo info = new UploadInfo();
        info.setEntityLength(100L);
        info.setOffset(0L);
        String id = Stores.create(uploadStore, info);
        assertTrue(Stores.lock(uploadStore, id));
        try {
            Thread.sleep(200);
            assertFalse(Stores.lock(uploadStore, id), "an idle lock was reclaimed with the timeout disabled");
            Stores.await(uploadStore.cleanupStaleLocks());
            assertFalse(Stores.lock(uploadStore, id), "the stale-lock sweep removed a lock with the timeout disabled");
        } finally {
            Stores.unlock(uploadStore, id);
            Stores.discard(uploadStore, id);
        }
    }
}
