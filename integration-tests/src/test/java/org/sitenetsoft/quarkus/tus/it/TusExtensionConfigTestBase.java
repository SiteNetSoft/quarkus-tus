package org.sitenetsoft.quarkus.tus.it;

import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;

/**
 * Configuration-related TUS tests (HTTP-only, no CDI injection required).
 * Extended by both {@code @QuarkusTest} and {@code @QuarkusIntegrationTest} subclasses.
 */
abstract class TusExtensionConfigTestBase {

    @Test
    void testTusExtensionHeaderMatchesConfig() {
        given()
                .when().options("/tus")
                .then()
                .statusCode(204)
                .header("Tus-Extension", containsString("creation"))
                .header("Tus-Extension", containsString("termination"))
                .header("Tus-Extension", containsString("checksum"));
    }

    @Test
    void testMaxSizeFromConfig() {
        given()
                .when().options("/tus")
                .then()
                .statusCode(204)
                .header("Tus-Max-Size", "104857600");
    }
}
