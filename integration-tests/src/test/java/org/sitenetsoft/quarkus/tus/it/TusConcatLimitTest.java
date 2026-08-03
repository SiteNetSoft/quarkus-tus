package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;

/**
 * Caps how many partials one concatenation may reference. The practical ceiling would
 * otherwise be the HTTP header size limit, which is not a deliberate bound on how much work a
 * single request may schedule.
 */
@QuarkusTest
@TestProfile(TusConcatLimitTest.SmallConcatLimitProfile.class)
class TusConcatLimitTest {

    private static final int MAX_PARTS = 3;

    public static class SmallConcatLimitProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.tus.max-concat-parts", String.valueOf(MAX_PARTS));
        }
    }

    private static String createCompletePartial(byte[] data) {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .header("Upload-Concat", "partial")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);

        return location;
    }

    private static String concatHeaderFor(int count) {
        List<String> locations = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            locations.add(createCompletePartial(("p" + i).getBytes()));
        }
        return "final; " + String.join(" ", locations);
    }

    @Test
    void atTheLimitIsAccepted() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Concat", concatHeaderFor(MAX_PARTS))
                .when().post("/tus")
                .then()
                .statusCode(201);
    }

    @Test
    void beyondTheLimitIsRejected() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Concat", concatHeaderFor(MAX_PARTS + 1))
                .when().post("/tus")
                .then()
                .statusCode(400);
    }
}
