package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;

/**
 * Without {@code quarkus.http.proxy.proxy-address-forwarding}, forwarding headers come from an
 * untrusted source and must not influence which bucket a request is counted against.
 * Honouring them made the limiter a no-op: a different value per request bought a fresh burst
 * every time, and each distinct value also added a bucket that was only swept after an hour.
 * <p>
 * Deliberately a single test, because every request in this profile shares the one bucket
 * keyed by the peer address.
 */
@QuarkusTest
@TestProfile(TusRateLimitSpoofingTest.UntrustedProxyProfile.class)
class TusRateLimitSpoofingTest {

    public static class UntrustedProxyProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.tus.rate-limit-enabled", "true",
                    "quarkus.tus.rate-limit-requests-per-minute", "6",
                    "quarkus.tus.rate-limit-burst-size", "3");
        }
    }

    @Test
    void spoofedForwardedForCannotBuyExtraBurst() {
        // Each request claims to be a different client; all really come from the same peer.
        for (int i = 0; i < 3; i++) {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", "100")
                    .header("X-Forwarded-For", "203.0.113." + i)
                    .when().post("/tus")
                    .then()
                    .statusCode(201);
        }

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .header("X-Forwarded-For", "203.0.113.99")
                .when().post("/tus")
                .then()
                .statusCode(429)
                .header("Retry-After", notNullValue());
    }
}
