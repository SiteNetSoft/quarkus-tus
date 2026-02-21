package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;

@QuarkusTest
@TestProfile(TusSseDisabledTest.SseDisabledProfile.class)
class TusSseDisabledTest {

    public static class SseDisabledProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.tus.sse-enabled", "false");
        }
    }

    @Test
    void testSseEventsEndpointReturns404() {
        given()
                .header("Accept", "text/event-stream")
                .when().get("/tus/events/550e8400-e29b-41d4-a716-446655440000")
                .then()
                .statusCode(404);
    }

    @Test
    void testSseProgressEndpointReturns404() {
        given()
                .header("Accept", "text/event-stream")
                .when().get("/tus/progress/550e8400-e29b-41d4-a716-446655440000")
                .then()
                .statusCode(404);
    }

    @Test
    void testCoreTusEndpointsStillWork() {
        // OPTIONS should still work
        given()
                .when().options("/tus")
                .then()
                .statusCode(204)
                .header("Tus-Resumable", notNullValue());

        // POST create should still work
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue());
    }
}
