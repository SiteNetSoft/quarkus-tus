package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(TusRateLimitTest.RateLimitProfile.class)
class TusRateLimitTest {

    public static class RateLimitProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.tus.rate-limit-enabled", "true",
                    "quarkus.tus.rate-limit-requests-per-minute", "6",
                    "quarkus.tus.rate-limit-burst-size", "3"
            );
        }
    }

    @Test
    void testPostWithinBurstSucceeds() {
        // 3 POSTs should all succeed within burst size
        for (int i = 0; i < 3; i++) {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", "100")
                    .when().post("/tus")
                    .then()
                    .statusCode(201);
        }
    }

    @Test
    void testPostExceedingBurstReturns429() {
        // Use a unique X-Forwarded-For to isolate from other tests' buckets
        String clientIp = "10.0.0.42";

        // Exhaust the burst
        for (int i = 0; i < 3; i++) {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("Upload-Length", "100")
                    .header("X-Forwarded-For", clientIp)
                    .when().post("/tus")
                    .then()
                    .statusCode(201);
        }

        // 4th POST should be rate limited
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .header("X-Forwarded-For", clientIp)
                .when().post("/tus")
                .then()
                .statusCode(429)
                .header("Retry-After", notNullValue());
    }

    @Test
    void testOptionsNotRateLimited() {
        String clientIp = "10.0.0.43";

        for (int i = 0; i < 20; i++) {
            given()
                    .header("X-Forwarded-For", clientIp)
                    .when().options("/tus")
                    .then()
                    .statusCode(204);
        }
    }

    @Test
    void testHeadNotRateLimited() {
        String clientIp = "10.0.0.44";

        // Create an upload to HEAD against
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .header("X-Forwarded-For", clientIp)
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        // Even though we consumed a burst token on POST, HEAD should not be rate limited
        // Use a fresh client IP for the HEAD requests to have full burst
        String headClientIp = "10.0.0.45";
        for (int i = 0; i < 20; i++) {
            given()
                    .header("Tus-Resumable", "1.0.0")
                    .header("X-Forwarded-For", headClientIp)
                    .when().head(location)
                    .then()
                    .statusCode(200);
        }
    }
}
