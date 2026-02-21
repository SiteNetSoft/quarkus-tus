package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;

@QuarkusTest
@TestProfile(TusAuthTest.AuthEnabledProfile.class)
class TusAuthTest {

    public static class AuthEnabledProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.tus.auth-enabled", "true");
        }
    }

    @Test
    void testOptionsAllowedWithoutAuth() {
        given()
                .when().options("/tus")
                .then()
                .statusCode(204);
    }

    @Test
    void testPostRequiresAuth() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .when().post("/tus")
                .then()
                .statusCode(401);
    }

    @Test
    void testHeadRequiresAuth() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head("/tus/550e8400-e29b-41d4-a716-446655440000")
                .then()
                .statusCode(401);
    }

    @Test
    void testPatchRequiresAuth() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body("test".getBytes())
                .when().patch("/tus/550e8400-e29b-41d4-a716-446655440000")
                .then()
                .statusCode(401);
    }

    @Test
    void testDeleteRequiresAuth() {
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete("/tus/550e8400-e29b-41d4-a716-446655440000")
                .then()
                .statusCode(401);
    }
}
