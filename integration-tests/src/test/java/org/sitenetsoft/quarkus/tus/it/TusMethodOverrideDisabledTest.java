package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;

/**
 * With {@code quarkus.tus.method-override-enabled=false} the filter is not registered at all,
 * so X-HTTP-Method-Override is inert and a POST to an upload URL is simply not allowed.
 */
@QuarkusTest
@TestProfile(TusMethodOverrideDisabledTest.MethodOverrideDisabledProfile.class)
class TusMethodOverrideDisabledTest {

    public static class MethodOverrideDisabledProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.tus.method-override-enabled", "false");
        }
    }

    private static String createUpload(int length) {
        return given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");
    }

    @Test
    void overrideDeleteIsIgnoredWhenDisabled() {
        String location = createUpload(100);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("X-HTTP-Method-Override", "DELETE")
                .when().post(location)
                .then()
                .statusCode(405);

        // The upload must still exist.
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200);
    }

    @Test
    void normalMethodsStillWorkWhenDisabled() {
        byte[] data = "still works".getBytes();
        String location = createUpload(data.length);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);
    }
}
